import sys

from dataclasses import dataclass
from typing import List

# import sqlparse - available if you need it!
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

def parse_record_serial(serial:int) -> int:
    if serial % 2 == 0:
        return (serial - 12) // 2
    elif serial % 2 == 1:
        return (serial - 13) // 2
    return -1

def get_num_tables(database_file):
    # skip the file header to num cells in page header
    # 100 = file header offset (bytes)
    # 3 = cell number offset
    database_file.seek(103)
    num_tables = int.from_bytes(database_file.read(2), byteorder="big")
    return num_tables

def get_table_names(database_file):
    num_tables = get_num_tables(database_file)
    # seek past page header - page header is 8 bytes, but 8th byte is empty
        # in non-interior pages
    database_file.seek(108)
    cell_offsets = [int.from_bytes(database_file.read(2), byteorder="big")
                    for _ in range(0, num_tables)]
    cell_contents = []
    for offset in cell_offsets:
        database_file.seek(offset)
        # how to read varint? bitwise operations, read first bit. if it's
        # 1, then the following byte is part of the same varint. if it is
        # 0, the current byte is the last byte in the varint
        # then save the num of bytes of the above 2 varints, add them to offset
        # to table name later
        record_size, next_offset = read_varint(database_file, [], offset)
        record_size_varint_size = next_offset - offset
        row_id, row_id_offset = read_varint(database_file, [], next_offset)
        row_id_varint_size = row_id_offset - next_offset
        next_offset = row_id_offset
        record_header_size, next_offset = read_varint(database_file, [],
                                                      next_offset)
        schema_type, next_offset = read_varint(database_file, [],
                                               next_offset)
        schema_name, next_offset = read_varint(database_file, [],
                                               next_offset)
        table_name, next_offset = read_varint(database_file, [],
                                              next_offset)
        bytes_to_table_name = parse_record_serial(schema_type) + parse_record_serial(schema_name)
        table_name_length = parse_record_serial(table_name)

        name_offset = offset + record_header_size + bytes_to_table_name + record_size_varint_size + row_id_varint_size
        database_file.seek(name_offset)
        cell_contents.append(database_file.read(table_name_length).decode("utf-8"))
    return cell_contents


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

        num_tables = get_num_tables(database_file)

        print(f"database page size: {page_size}")
        print(f"number of tables: {num_tables}")

elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        table_names = get_table_names(database_file)
        print(f"{" ".join(table_names)}")
else:
    print(f"Invalid command: {command}")


