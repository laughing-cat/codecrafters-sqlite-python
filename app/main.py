import sys
# import sqlparse

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
        self.pages = []
        self.table_to_root_page = {}
        # parse file header
        database_file.seek(16)  # Skip the first 16 bytes of the header
        self.page_size = int.from_bytes(database_file.read(2), byteorder="big")
        # in-header database size in pages
        database_file.seek(28)
        self.database_size = int.from_bytes(database_file.read(4),
                                            byteorder="big")
        # parse every page
        for i in range(self.database_size):
            page_offset = i * self.page_size
            if page_offset == 0:
                page_offset = 100
            page = Page(database_file, page_offset)
            self.pages.append(page)
        
        # parse schema table
        schema_table = self.pages[0]
        for cell in schema_table.cells:
            name = cell.content[2].decode("utf-8")
            root_page:int = int.from_bytes(cell.content[3], byteorder = "big")
            self.table_to_root_page[name] = root_page
    
    def get_rows_in_table(self, table_name):
        table_root_page = self.table_to_root_page[table_name]
        if table_root_page is None:
            print(f"table not found: {table_name}")
            return -1
        return self.pages[table_root_page - 1].num_cells

    def table_count(self):
        return self.pages[0].num_cells


    

class Page:
    def __init__(self, database_file, page_offset):
        self.cells = []
        database_file.seek(page_offset)
        self.page_type = int.from_bytes(database_file.read(1), byteorder='big')
        self.first_freeblock = int.from_bytes(database_file.read(2),
                                              byteorder='big')
        self.num_cells = int.from_bytes(database_file.read(2), byteorder='big')
        self.start_content = int.from_bytes(database_file.read(2), byteorder='big')
        if self.start_content == 0:
            self.start_content = 65536
        self.free_bytes = int.from_bytes(database_file.read(1), byteorder='big')
        # TODO: parse right-most pointer here if interior b-tree page
        
        # parse cells of page
        cell_offsets = [int.from_bytes(database_file.read(2), byteorder="big")
                    for _ in range(0, self.num_cells)]

        for offset in cell_offsets:
            database_file.seek(offset)
            record = Record(database_file, offset)
            self.cells.append(record)
        

# Record -> row of table
# https://www.sqlite.org/fileformat.html#record_format
class Record:
    # Currently assumes the cell is a table B-Tree Leaf Cell
    def __init__(self, database_file, cell_offset):
        self.offset: int = cell_offset
        self.column_sizes: list[int] = []
        self.content: dict[int, bytes] = dict()

        # parse cell header
        record_size, next_offset = read_varint(database_file, [], cell_offset)
        self.record_size: int = record_size

        row_id, next_offset = read_varint(database_file, [], next_offset)
        self.row_id: int = row_id

        # start of record
        record_offset = next_offset
        record_header_size, next_offset = read_varint(database_file, [],
                                                      next_offset)
        self.record_header_size: int = record_header_size

        cur_offset = next_offset
        while cur_offset - record_offset < record_header_size:
            # read next varint, parse serial type to get content_size
            next_serial, cur_offset = read_varint(database_file, [], cur_offset)
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

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        database = Database(database_file)
        page_size = database.page_size

        num_tables = database.table_count()

        print(f"database page size: {page_size}")
        print(f"number of tables: {num_tables}")

elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        database = Database(database_file)
        table_names = database.table_to_root_page.keys()
        print(f"{' '.join(table_names)}")
elif "select" in command or "SELECT" in command:
    if "count" in command or "COUNT" in command:
        with open(database_file_path, "rb") as database_file:
            table_name = command.split(" ")[-1]
            database = Database(database_file)
            print(f"{database.get_rows_in_table(table_name)}")
    else:
        with open(database_file_path, "rb") as database_file:
            table_name = command.split(" ")[-1]
            # Get col from table
            database = Database(database_file)

else:
    print(f"Invalid command: {command}")


