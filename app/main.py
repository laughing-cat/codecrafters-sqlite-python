from enum import Enum
import sys
import sqlparse
from sqlparse.sql import Function, Identifier, Where

# how to read varint? bitwise operations, read first bit. if it's
# 1, then the following byte is part of the same varint. if it is
# 0, the current byte is the last byte in the varint
def read_varint(stream, bytes_list, offset: int):
    # check msb of current byte 
    stream.seek(offset)
    next_offset = offset
    while True:
        byte = ord(stream.read(1))
        next_offset = next_offset + 1
        if byte >> 7 == 1:
            bytes_list.append(format((byte ^ 128), '07b'))
        else:
            bytes_list.append(format(byte, '07b'))
            break

    result = int(''.join(bytes_list), 2)
    return (result, next_offset)



# Database file representation: https://www.sqlite.org/fileformat.html
class Database:
    def __init__(self, database_file):
        self.pages = dict()
        self.table_to_metadata = {}
        self.index_to_table = {}
        # parse file header
        database_file.seek(16)  # Skip the first 16 bytes of the header
        self.page_size = int.from_bytes(database_file.read(2), byteorder="big")
        # in-header database size in pages
        database_file.seek(28)
        self.database_size = int.from_bytes(database_file.read(4), byteorder="big")

        # TODO: schema table could be multiple pages too if there are lots of entries
        #TODO: read index tables
        self.schema_table = Page(database_file, 100)
        for cell in self.schema_table.cells:
            table_type = cell.content[0].decode("utf-8")
            name = cell.content[2].decode("utf-8")
            root_page:int = int.from_bytes(cell.content[3], byteorder = "big")
            sql_query = cell.content[4].decode("utf-8")
            if table_type == "index":
                self.table_to_metadata[table_type + name] = TableMetadata(table_type, root_page, sql_query)
                parsed = sqlparse.parse(sql_query)[0]
                table_name, index_col = parsed.token_prev(len(parsed.tokens))[1].value.split(" ")
                index_col = index_col[1:len(index_col) - 1]
                self.index_to_table[index_col] = table_name
            else:
                self.table_to_metadata[name] = TableMetadata(table_type, root_page, sql_query)

        # parse every root page
        # can't parse every page indiscriminately since not every page is a b-tree page.
        # Need to parse first page (schema page), and then only parse the root pages of each table
        # Thus pages needs to be changed to a map
        for metadata in self.table_to_metadata.values():
            root_page = metadata.root_page
            page_offset = (root_page-1) * self.page_size
            page = Page(database_file, page_offset)
            self.pages[root_page] = page
    
    def get_rows_in_table(self, table_name):
        table_metadata = self.table_to_metadata[table_name]
        if table_metadata is None:
            print(f"table not found: {table_name}")
            return -1
        return self.pages[table_metadata.root_page].num_cells

    def table_count(self):
        return self.schema_table.num_cells

    
    def get_col_values_from_table(self, col_list, table_name, filters, ids={}):
        table_metadata = self.table_to_metadata[table_name]
        if table_metadata is None:
            print(f"table not found: {table_name}")
            return []
        col_indices = []
        col_names = [col.value for col in col_list]
        filter_index = None
        for col in col_names:
            for index, name in enumerate(table_metadata.col_names):
                if col in name:
                    col_indices.append(index)
                    break

        index_table_metadata = None
        for index, name in enumerate(table_metadata.col_names):
            for filter in filters:
                if len(self.index_to_table) > 0 and self.index_to_table[filter[0]] is not None:
                    index_table_metadata = self.table_to_metadata["index" + self.index_to_table[filter[0]]] 
                if filter[0] in name:
                    filter_index = index
        if len(col_indices) == 0:
            print(f"no col names with names: {col_names} found")
            return []
        root_page = self.pages[table_metadata.root_page] if index_table_metadata is None or len(ids) > 0 else self.pages[index_table_metadata.root_page]
        # go to root page
        # if root page is a leaf table, no need to traverse
        # if root page is an interior table, then need to
        # continue until a leaf table page is found

        result = {}
        page_stack = [root_page]
        row_ids = []
        filter = filters[0] if len(filters) > 0 else (None,None)
        filter_value = filter[1]
        has_id_filter = True if len(ids) > 0 else False
        
        while len(page_stack) > 0:
            page = page_stack.pop()
            #TODO: handle cell overflow
            if page.page_type == PageType.LEAF_TABLE:
                for cell in page.cells:
                    if filter_index is not None:
                        value = cell.content[filter_index].decode("utf-8")
                        if value != filter_value: continue
                    elif len(ids) > 0:
                        id_match = cell.row_id in ids
                        if not id_match: continue
                    current = []
                    for col_index in col_indices:
                        curr_value = cell.content[col_index].decode("utf-8")
                        if col_index == 0:
                            # id col is stored in row_id
                            curr_value = str(cell.row_id)
                        current.append(curr_value)
                    result[cell.row_id] = ("|".join(current))
            elif page.page_type == PageType.INTERIOR_TABLE:
                # Push all child pages onto the stack, search for page with index,
                if has_id_filter:
                    for search_id in ids:
                        leftmost_rowid = page.cells[0].row_id
                        rightmost_rowid = page.cells[-1].row_id
                        if search_id < leftmost_rowid:
                                page_offset = (page.cells[0].left_child_pointer - 1) * self.page_size
                                child_page = Page(database_file, page_offset)
                                page_stack.append(child_page)
                                continue
                        if search_id > rightmost_rowid:
                                page_offset = (page.rightmost_pointer - 1) * self.page_size
                                child_page = Page(database_file, page_offset)
                                page_stack.append(child_page)
                                continue
                        l, r = 0, len(page.cells)
                        while l < r:
                            mid = (l + r)//2
                            cell = page.cells[mid]
                            if cell.row_id > search_id:
                                page_offset = (cell.left_child_pointer - 1) * self.page_size
                                child_page = Page(database_file, page_offset)
                                page_stack.append(child_page)
                                r = mid - 1
                            elif cell.row_id < search_id:
                                if mid + 1 < len(page.cells):
                                    cell = page.cells[mid + 1]
                                    page_offset = (cell.left_child_pointer - 1) * self.page_size
                                    child_page = Page(database_file, page_offset)
                                    page_stack.append(child_page)
                                else:
                                    page_offset = (page.rightmost_pointer - 1) * self.page_size
                                    child_page = Page(database_file, page_offset)
                                    page_stack.append(child_page)
                                l = mid + 1
                # or search every page if index is not available
                else:
                    for cell in page.cells:
                        page_offset = (cell.left_child_pointer - 1) * self.page_size
                        child_page = Page(database_file, page_offset)
                        page_stack.append(child_page)
            elif page.page_type == PageType.INTERIOR_INDEX:
                # Look for pages with row ids in index
                # binary search
                l, r, = 0, len(page.cells)
                while l < r:
                    mid = (l + r)//2
                    cell = page.cells[mid]
                    if cell.content["col_name"] > filter_value:
                        page_offset = (cell.left_child_pointer - 1) * self.page_size
                        child_page = Page(database_file, page_offset)
                        page_stack.append(child_page)
                        r = mid - 1
                    elif cell.content["col_name"] < filter_value:
                        if mid + 1 < len(page.cells):
                            cell = page.cells[mid + 1]
                            page_offset = (cell.left_child_pointer - 1) * self.page_size
                            child_page = Page(database_file, page_offset)
                            page_stack.append(child_page)
                        else:
                            page_offset = (page.rightmost_pointer - 1) * self.page_size
                            child_page = Page(database_file, page_offset)
                            page_stack.append(child_page)
                        l = mid + 1
                    else:
                        row_ids.append(cell.content["row_id"])
            elif page.page_type == PageType.LEAF_INDEX:
                # binary search through cells
                l, r, = 0, len(page.cells)
                while l < r:
                    mid = (l + r)//2
                    cell = page.cells[mid]
                    if cell.content["col_name"] > filter_value:
                        r = mid - 1
                    elif cell.content["col_name"] < filter_value:
                        l = mid + 1
                    else:
                        start = mid
                        while cell and cell.content["col_name"] == filter_value:
                            row_ids.append(cell.content["row_id"])
                            mid = mid - 1
                            cell = page.cells[mid] if mid >= 0 else None
                        mid = start + 1
                        cell = page.cells[mid]
                        while cell and cell.content["col_name"] == filter_value:
                            row_ids.append(cell.content["row_id"])
                            mid = mid + 1
                            cell = page.cells[mid] if mid < len(page.cells) else None
                        l = r
        if len(row_ids) > 0 and len(ids) == 0:
            # TODO: this is executing an unnecessary amount of times causing the result to have to be a set to dedupe, but can optimize
            result = self.get_col_values_from_table(col_list, table_name, filters, row_ids)
            return result

        return result.values()

class TableMetadata:
    def __init__(self, table_type, root_page, sql_query):
        self.table_type = table_type
        self.root_page = root_page
        self.sql_query = sql_query
        query = sqlparse.parse(self.sql_query)[0]
        self.col_names = query[-1].value.split(',')

class PageType(Enum):
    INTERIOR_INDEX = 2
    INTERIOR_TABLE = 5
    LEAF_INDEX = 10
    LEAF_TABLE = 13

class Page:
    def __init__(self, database_file, page_offset):
        self.cells = []
        database_file.seek(page_offset)
        try:
            self.page_type = PageType(int.from_bytes(database_file.read(1),
                                                     byteorder='big'))
        except:
            print("Not a b-tree page.")
            return
        self.first_freeblock = int.from_bytes(database_file.read(2),
                                              byteorder='big')
        self.num_cells = int.from_bytes(database_file.read(2), byteorder='big')
        self.start_content = int.from_bytes(database_file.read(2), byteorder='big')
        if self.start_content == 0:
            self.start_content = 65536
        self.free_bytes = int.from_bytes(database_file.read(1), byteorder='big')
        if self.page_type == PageType.INTERIOR_TABLE or self.page_type == PageType.INTERIOR_INDEX:
            self.rightmost_pointer = int.from_bytes(database_file.read(4), byteorder='big')
        
        # parse cells of page
        cell_offsets = [int.from_bytes(database_file.read(2), byteorder="big")
                    for _ in range(0, self.num_cells)]

        for offset in cell_offsets:
            # NOTE: offsets are relative to start of page, need to add page number * page size
            if page_offset != 100:
                offset += page_offset
            database_file.seek(offset)
            cell = None
            if self.page_type == PageType.LEAF_TABLE: 
                cell = LeafTableCell(database_file, offset)
            elif self.page_type == PageType.INTERIOR_INDEX:
                cell = InteriorIndexCell(database_file, offset)
                # print(cell.content)
            elif self.page_type == PageType.LEAF_INDEX:
                cell = LeafIndexCell(database_file, offset)
            elif self.page_type == PageType.INTERIOR_TABLE:
                cell = InteriorTableCell(database_file, offset)
            self.cells.append(cell)

class Cell:
    def __init__(self, database_file, cell_offset):
        self.offset: int = cell_offset
        database_file.seek(cell_offset)

class InteriorIndexCell(Cell):
    def __init__(self, database_file, cell_offset):
        super().__init__(database_file, cell_offset)
        self.left_child_pointer: int = int.from_bytes(database_file.read(4), byteorder="big")
        payload_size, next_offset = read_varint(database_file, [], cell_offset + 4)
        self.payload_size = payload_size
        self.record = Record(database_file, next_offset)
        self.content = {"col_name":self.record.content[0].decode("utf-8"), "row_id":int.from_bytes(self.record.content[1], byteorder="big")}

class LeafIndexCell(Cell):
    def __init__(self, database_file, cell_offset):
        super().__init__(database_file, cell_offset)
        payload_size, next_offset = read_varint(database_file, [], cell_offset)
        self.payload_size = payload_size
        self.record = Record(database_file, next_offset)
        self.content = {"col_name":self.record.content[0].decode("utf-8"), "row_id":int.from_bytes(self.record.content[1], byteorder="big")}


class InteriorTableCell(Cell):
    def __init__(self, database_file, cell_offset):
        super().__init__(database_file, cell_offset)
        self.left_child_pointer: int = int.from_bytes(database_file.read(4), byteorder="big")
        integer_key, next_offset = read_varint(database_file, [], cell_offset + 4)
        self.row_id: int = integer_key

class LeafTableCell(Cell):
    def __init__(self, database_file, cell_offset):
        super().__init__(database_file, cell_offset)
        # parse cell header
        record_size, next_offset = read_varint(database_file, [], cell_offset)
        self.record_size: int = record_size

        row_id, next_offset = read_varint(database_file, [], next_offset)
        self.row_id: int = row_id
        self.record = Record(database_file, next_offset)
        self.content = self.record.content


# Record -> row of table for table b-tree data or index b-tree keys
# https://www.sqlite.org/fileformat.html#record_format
class Record:
    def __init__(self, database_file, offset):
        self.offset: int = offset
        database_file.seek(offset)
        self.column_sizes: list[int] = []
        self.content: dict[int, bytes] = dict()

        # start of record
        record_header_size, next_offset = read_varint(database_file, [],
                                                      offset)
        self.record_header_size: int = record_header_size

        cur_offset = next_offset
        while cur_offset - offset < record_header_size:
            # read next varint, parse serial type to get content_size
            next_serial, cur_offset = read_varint(database_file, [], cur_offset)
            # TODO: handle int values, not just strings
            content_size = self.parse_serial(next_serial)
            self.column_sizes.append(content_size)

        # parse record content
        for index, size in enumerate(self.column_sizes):
            database_file.seek(cur_offset)
            content = database_file.read(size)
            self.content[index] = content
            cur_offset += size

        # self.root_page:int = int.from_bytes(self.content[3], byteorder = "big")
        # self.create_table_statement = self.content[4].decode('utf-8')
        # TODO: Get overflow page number
        # database_file.seek(next_offset)
        # self.overflow_page_number: int = int.from_bytes(database_file.read(4),
        #                                                 byteorder = "big")
    def parse_serial(self, serial:int) -> int:
        if serial == 0:
            # Value is a NULL
            return 0
        elif serial == 1:
            # Value is an 8-bit twos-complement integer
            return 1
        elif serial == 2:
            return 2
        elif serial == 3:
            return 3
        elif serial == 4:
            return 4
        elif serial == 5:
            return 6
        elif serial == 6:
            return 8
        elif serial == 7:
            return 8
        elif serial == 8:
            return 0
        elif serial == 9:
            return 0
        elif serial % 2 == 0:
            return (serial - 12) // 2
        elif serial % 2 == 1:
            return (serial - 13) // 2
        else:
            return -1

database_file_path = sys.argv[1]
command = sys.argv[2]
parsed_command = sqlparse.parse(command)[0]

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        database = Database(database_file)
        page_size = database.page_size

        num_tables = database.table_count()

        print(f"database page size: {page_size}")
        print(f"number of pages {database.database_size}")
        print(f"number of tables: {num_tables}")

elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        database = Database(database_file)
        table_names = database.table_to_metadata.keys()
        print(f"{' '.join(table_names)}")
# TODO: Use sqlparse to parse sql commands
elif "select" in command or "SELECT" in command:
    if isinstance(parsed_command.tokens[2], Function):
        with open(database_file_path, "rb") as database_file:
            table_name = command.split(" ")[-1]
            database = Database(database_file)
            print(f"{database.get_rows_in_table(table_name)}")
    else:
        with open(database_file_path, "rb") as database_file:
            parsed = sqlparse.parse(command)[0]
            columns = parsed.tokens[2]
            col_names = []
            if isinstance(columns, Identifier):
                col_names.append(columns)
            else:
                col_names = [token for token in parsed.tokens[2].get_identifiers()]
            last_token = parsed.token_prev(len(parsed.tokens))
            tokens = command.split(" ")
            table_name = tokens[-1]
            filters = []
            if isinstance(last_token, tuple) and isinstance(last_token[1],Where):
                where_tokens = [token for token in last_token[1].flatten()]
                col_filter = where_tokens[2].value
                filter_value = where_tokens[-1].value.replace("\'", "")
                filters.append((col_filter, filter_value))
                #TODO: fix the typing, this should always be a tuple though
                table_name = parsed.token_prev(len(parsed.tokens) - 1)[1].value
            database = Database(database_file)
            values = database.get_col_values_from_table(col_names, table_name, filters)
            print("\n".join(values))

else:
    print(f"Invalid command: {command}")


