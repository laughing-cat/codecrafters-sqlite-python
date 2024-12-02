import sys
# import sqlparse

# how to read varint? bitwise operations, read first bit. if it's
# 1, then the following byte is part of the same varint. if it is
# 0, the current byte is the last byte in the varint
def read_varint(stream, bytes_list, offset: int):
    # check msb of current byte 
    stream.seek(offset)
    byte = int.from_bytes(stream.read(1), "big")
    read_next = byte >> 7 == 1
    if read_next:
        bytes_list.append(byte ^ 0b10000000) #xor to toggle msb
        return read_varint(stream, bytes_list, offset + 1)
    bytes_list.append(byte)
    return (int.from_bytes(bytes(bytes_list), byteorder="big"), offset + 1)

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

        self.root_page:int = int.from_bytes(self.content[3], byteorder = "big")
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

def get_cell_offsets(database_file):
    num_tables = get_num_tables(database_file, 100)
    # seek past page header - page header is 8 bytes, but 8th byte is empty
        # in non-interior pages
    database_file.seek(108)
    cell_offsets = [int.from_bytes(database_file.read(2), byteorder="big")
                    for _ in range(0, num_tables)]
    return cell_offsets

def get_num_tables(database_file, page_start_offset):
    # skip the file header to num cells in page header
    # 100 = file header offset (bytes)
    # 3 = cell number offset
    database_file.seek(page_start_offset + 3)
    num_tables = int.from_bytes(database_file.read(2), byteorder="big")
    return num_tables

def get_table_names(database_file):
    cell_offsets = get_cell_offsets(database_file)
    cell_contents = []
    for offset in cell_offsets:
        record = Record(database_file, offset)
        cell_contents.append(record.content[2].decode("utf-8"))
    return cell_contents

def get_row_count_in_table(database_file, table_name):
    cell_offsets = get_cell_offsets(database_file)
    row_count = 0
    page_number = 0
    for offset in cell_offsets:
        record = Record(database_file, offset)
        name = record.content[2].decode("utf-8")
        if name == table_name:
            page_number = record.root_page - 1 # root page is 1-indexed
            break
    # go to page
    return get_num_tables(database_file, page_number * 4096)


database_file_path = sys.argv[1]
command = sys.argv[2]

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        # You can use print statements as follows for debugging, they'll be visible when running tests.
        print("Logs from your program will appear here!", file=sys.stderr)

        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        database_file.seek(100)
        print(f"page header: {int.from_bytes(database_file.read(1), byteorder='big')}")

        num_tables = get_num_tables(database_file, 100)

        print(f"database page size: {page_size}")
        print(f"number of tables: {num_tables}")

elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        table_names = get_table_names(database_file)
        print(f"{' '.join(table_names)}")
elif "select" in command or "SELECT" in command:
    with open(database_file_path, "rb") as database_file:
        table_name = command.split(" ")[-1]
        print(f"{get_row_count_in_table(database_file, table_name)}")
else:
    print(f"Invalid command: {command}")


