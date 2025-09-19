import argparse
import logging
import math
import multiprocessing
import os
import re
from math import floor
from typing import Union

from nbtlib import CompoundSchema, File, load, schema
from nbtlib.tag import Compound, Int, List, String
import numpy as np
from tqdm import tqdm

SCHEMATIC_VERSION = 2586

# Use https://github.com/JoshuaVandaele/Schem-File-to-Structure-Block-NBT-Format/tree/main to get info
# to convert a schematic file:
#   python schem2nbt.py -i <schem_file>
def structure_schema() -> CompoundSchema:
    """Generate a structure schema.

    Returns:
        CompoundSchema: The structure schema.
    """
    return schema(
        "Structure",
        {
            "DataVersion": Int,
            "author": String,
            "size": List[Int],
            "palette": List[
                schema(
                    "State",
                    {
                        "Name": String,
                        "Properties": Compound,
                    },
                )
            ],
            "blocks": List[
                schema(
                    "Block",
                    {
                        "state": Int,
                        "pos": List[Int],
                        "nbt": Compound,
                    },
                )
            ],
        },
    )()


def get_schematic_size(worldedit: File) -> dict[str, int]:
    """Gets the size of a worldedit schematic file.

    Args:
        worldedit (File): The worldedit schematic file.

    Returns:
        dict[str, int]: A dictionary containing the size of the schematic in the x, y, and z directions.
    """
    x, y, z = int(worldedit["Length"]), int(worldedit["Height"]), int(worldedit["Width"])
    # x = min(x, 48)
    # y = min(y, 48)
    # z = min(z, 48)

    return {
        "x": x,
        "y": y,
        "z": z
    }


def initiate_schema(worldedit: File) -> CompoundSchema:
    """Initiates a structure file.

    Args:
        worldedit (File): The worldedit schematic file to base the structure file off of.

    Returns:
        CompoundSchema: The structure file.
    """
    nbt_schematic: CompoundSchema = structure_schema()
    nbt_schematic["DataVersion"] = SCHEMATIC_VERSION
    nbt_schematic["palette"] = []
    nbt_schematic["blocks"] = []
    nbt_schematic["author"] = "Folfy_Blue"

    size: dict[str, int] = get_schematic_size(worldedit)

    nbt_schematic["size"] = [size["x"], size["y"], size["z"]]
    return nbt_schematic

# Personal
def initiate_schema_from_size(size: dict[str, int]) -> CompoundSchema:
    """Initiates a structure file.

    Args:
        worldedit (File): The worldedit schematic file to base the structure file off of.

    Returns:
        CompoundSchema: The structure file.
    """
    nbt_schematic: CompoundSchema = structure_schema()
    nbt_schematic["DataVersion"] = SCHEMATIC_VERSION
    nbt_schematic["palette"] = []
    nbt_schematic["blocks"] = []
    nbt_schematic["author"] = "Folfy_Blue"

    nbt_schematic["size"] = [size["x"], size["y"], size["z"]]
    return nbt_schematic

def get_block_palette(worldedit: File) -> dict[int, str]:
    """Gets the block palette from a worldedit schematic file and returns it as a dictionary.

    Args:
        worldedit (File): The worldedit schematic file.

    Returns:
        dict[int, str]: A dictionary of block palette entries.
    """
    return {int(v): k for k, v in dict(worldedit["Palette"]).items()}


def process_block_palette(
    nbt_schematic: CompoundSchema, byte_palette: dict[int, str]
) -> tuple[CompoundSchema, dict[str, int]]:
    """Processes the block palette from a worldedit palette and returns it in a structure file format.

    Args:
        nbt_schematic (CompoundSchema): The structure file.
        byte_palette (dict): The block palette.

    Returns:
        tuple[CompoundSchema, dict]: A tuple containing the structure file and the new palette.
    """
    new_palette = {}
    for _palette, block in byte_palette.items():
        block_name, block_properties = re.findall(r"(minecraft:\w+)(\[.+\])?", block)[0]
        block_properties = re.findall(r"((\w+)=(\w+))", block_properties)
        bp = {}
        for block_property in block_properties:
            bp[block_property[1]] = String(block_property[2])
        if len(bp) > 0:
            nbt_schematic["palette"].append({"Name": block_name, "Properties": bp})
        else:
            nbt_schematic["palette"].append({"Name": block_name})
        new_palette[block] = len(nbt_schematic["palette"]) - 1

    return nbt_schematic, new_palette


# def process_blocks(
#     worldedit: File,
#     nbt_schematic: CompoundSchema,
#     byte_palette: dict[int, str],
#     new_palette: dict[str, int],
#     block_entities: dict[str, Compound] = {},
#     queue: Union[multiprocessing.Queue, None] = None,
# ) -> CompoundSchema:
#     """Processes blocks from a worldedit schematic file and returns them in a structure file format.

#     Args:
#         worldedit (File): The worldedit schematic file.
#         nbt_schematic (CompoundSchema): The structure file.
#         byte_palette (dict[int, str]): The old block palette from world edit.
#         new_palette (dict[str, int]): The new block palette to use.
#         input_file (str, optional): The name of the input file, used for the loading bar. Defaults to "".
#         block_entities (dict[str, Compound], optional): The block entities. If empty, they will be devoid of nbt. Defaults to {}.
#         queue (Union[multiprocessing.Queue, None], optional): The queue to use for the loading bar. Defaults to None.

#     Returns:
#         CompoundSchema: The structure file.
#     """

#     # Note: block entiies is the set of block entities

#     size: dict[str, int] = get_schematic_size(worldedit)

#     for i in range(len(worldedit["BlockData"])):
#         x = floor((i % (size["z"] * size["x"])) % size["z"])
#         y = floor(i / (size["z"] * size["x"]))
#         z = floor((i % (size["z"] * size["x"])) / size["z"])
#         key: str = f"{x} {y} {z}"

#         block_id = int(worldedit["BlockData"][i])

#         try:
#             block: str = byte_palette[block_id]
#         except KeyError:
#             block: str = byte_palette[0]
#             logging.warning(
#                 f"We couldn't process the block at {key}: Block {block_id} doesn't exist. Defaulting to block 0 ({block})."
#             )

#         if key in block_entities:
#             nbt_schematic["blocks"].append(
#                 {
#                     "state": new_palette[block],
#                     "pos": [x, y, z],
#                     "nbt": block_entities[key],
#                 }
#             )
#         else:
#             nbt_schematic["blocks"].append(
#                 {"state": new_palette[block], "pos": [x, y, z]}
#             )
#         if queue:
#             queue.put(True)

#     return nbt_schematic

def process_single_block(
    worldedit: File,
    nbt_schematic: CompoundSchema,
    byte_palette: dict[int, str],
    new_palette: dict[str, int],
    block_entities: dict[str, Compound] = {},
    single_block_name: str = "lime_stained_glass",
    queue: Union[multiprocessing.Queue, None] = None,
) -> list:

    size: dict[str, int] = get_schematic_size(worldedit)
    
    region_array=[]

    # tuple split into x, y, z
    region_nums = [math.ceil(size["x"]/48), math.ceil(size["y"]/48), math.ceil(size["z"]/48)]



    # Creates empty 3d array of right size
    for x_index in range(region_nums[0]):
        region_array.append([])
        for y_index in range(region_nums[1]):
            region_array[x_index].append([])
            for z_index in range(region_nums[2]):
                nbt_schematic_temp = initiate_schema_from_size(size=size)
                region_array[x_index][y_index].append([])
                region_array[x_index][y_index][z_index] = nbt_schematic_temp




    # region_array = np.empty(region_nums)

    for i in range(len(worldedit["BlockData"])):
        x = floor((i % (size["z"] * size["x"])) % size["z"])
        y = floor(i / (size["z"] * size["x"]))
        z = floor((i % (size["z"] * size["x"])) / size["z"])
        key: str = f"{x % 48} {y % 48} {z % 48}"

        block_id = int(worldedit["BlockData"][i])

        try:
            block: str = byte_palette[block_id]
        except KeyError:
            block: str = byte_palette[0]
            logging.warning(
                f"We couldn't process the block at {key}: Block {block_id} doesn't exist. Defaulting to block 0 ({block})."
            )
        


        # appends a new block to the nbt if it matches
        if block == single_block_name:
            region_array[x // 48][y // 48][z // 48]["blocks"].append(
                    {"state": new_palette[block], "pos": [x % 48, y % 48, z % 48]}
                )



        # Not quite sure what this does
        if queue:
            queue.put(True)

    # For the record, region_array is a 3D array of nbt_schematic (Class CompoundSchema)
    return region_array


def process_file(
    input_file: str, output_file: str, queue=Union[multiprocessing.Queue, None]
) -> None:
    """Processes a worldedit schematic file and saves it as a structure file.

    Args:
        input_file (str): The worldedit schematic file.
        output_file (str): The structure file.
        queue (Union[multiprocessing.Queue, None], optional): The queue to use for the loading bar. Defaults to None.
    """
    logging.info(f"Processing {input_file}...")
    try:
        file_name, nbt = output_file.split(".")

        # Makes Directory
        try:
            os.mkdir(file_name)
        except FileExistsError:
            print(f"Directory '{file_name}' already exists.")
        except PermissionError:
            print(f"Permission denied: Unable to create '{file_name}'.")
        except Exception as e:
            print(f"An error occurred: {e}")

        with load(input_file) as worldedit:
            nbt_schematic: CompoundSchema = initiate_schema(worldedit)

            block_entities = {}
            byte_palette = get_block_palette(worldedit)

            temp_schem, full_palette = process_block_palette(
                    nbt_schematic, byte_palette
                )

            for block in full_palette:
                if block == "minecraft:air":
                    continue

                nbt_schematic: CompoundSchema = initiate_schema(worldedit)
                nbt_schematic, new_palette = process_block_palette(
                    nbt_schematic, byte_palette
                )

                region_list = process_single_block(
                    worldedit=worldedit,
                    nbt_schematic=nbt_schematic,
                    byte_palette=byte_palette,
                    new_palette=new_palette,
                    block_entities=block_entities,
                    single_block_name=block,
                    queue=queue,  # type: ignore - The type checker doesn't like multiprocessing.Queue
                )
                logging.info(f"Saving {output_file}...")
                # output_file == "yeast.nbt", block == "minecraft:name". The following variables are text
                #file_name, nbt = output_file.split(".")
                minecraft, block_name = block.split(":")
                
                directory_name = block_name
                try:
                    os.mkdir(file_name + "/" + directory_name)
                    print(f"Directory '{directory_name}' created successfully.")
                except FileExistsError:
                    print(f"Directory '{directory_name}' already exists.")
                except PermissionError:
                    print(f"Permission denied: Unable to create '{directory_name}'.")
                except Exception as e:
                    print(f"An error occurred: {e}")

                # Create a file that has 
                mcfunction_filler = open(f"{file_name}/{directory_name}/build.mcfunction", "w")

                for i in range(len(region_list)):
                    for j in range(len(region_list[i])):
                        for k in range(len(region_list[i][j])):
                            File({"": Compound(nbt_schematic)}, gzipped=True).save(f"{file_name}/{directory_name}/{i}_{j}_{k}" + ".nbt")
                            mcfunction_filler.write(f"place template celledit:{file_name}/{directory_name}/{i}_{j}_{k} ~{48 * i} ~{48 * j} ~{48 * k}\n")
                
                mcfunction_breaker = open(f"{file_name}/{directory_name}/destroy.mcfunction", "w")
                
                for i in range(len(region_list)):
                    for j in range(len(region_list[i])):
                        for k in range(len(region_list[i][j])):
                            mcfunction_breaker.write(f"fill ~{32*i} ~{32*j} ~{32*k} ~{32*i + 31} ~{32*j + 31} ~{32*k + 31} air replace minecraft:{block_name}\n")
                break

                


    except Exception as e:
        logging.error(f"An error occurred while processing {input_file}: {repr(e)}")


def process_files(input_files: list[str], output_files: list[str]) -> None:
    """Process files from input and output them to given locations.

    Args:
        input_files (list[str]): The input files.
        output_files (list[str]): The output files.
    """
    queue = multiprocessing.Queue()
    processes = []

    total_blocks = 0
    for input_file in input_files:
        with load(input_file) as worldedit:
            total_blocks += len(worldedit["BlockData"])

    with tqdm(total=total_blocks, desc="Blocks processed") as pbar:
        for input_file, output_file in zip(input_files, output_files):
            process = multiprocessing.Process(
                target=process_file, args=(input_file, output_file, queue)
            )
            processes.append(process)
            process.start()

        while any(process.is_alive() for process in processes):
            while not queue.empty():
                queue.get()
                pbar.update()

        for process in processes:
            process.join()

        pbar.close()


def process_paths(args: argparse.Namespace) -> tuple[list[str], list[str]]:
    """Process input and output paths according to provided arguments.

    Args:
        args (argparse.Namespace): The arguments provided by the user.

    Returns:
        tuple[list[str], list[str]]: A tuple containing the input and output paths.
    """
    input_files, output_files = [], []
    if args.folder:
        # input_path is a directory
        if not os.path.exists(args.input):
            logging.error(f"Folder '{args.input}' not found.")
            exit(1)
        if not args.output:
            args.output = args.input
        os.makedirs(args.output, exist_ok=True)
        file_list = [
            f
            for f in os.listdir(args.input)
            if os.path.isfile(os.path.join(args.input, f))
        ]
        input_files = [os.path.join(args.input, f) for f in file_list]
        output_files = [
            os.path.join(args.output, f"{os.path.splitext(f)[0]}.nbt")
            for f in file_list
        ]
    else:
        # input_path is a file or doesn't exist
        if not os.path.isfile(args.input):
            logging.error(f"File '{args.input}' not found.")
            exit(1)
        input_files = [args.input]
        output_files = [args.output or f"{os.path.splitext(args.input)[0]}.nbt"]

    return input_files, output_files


def main() -> None:
    """Main function to execute the script."""
    parser = argparse.ArgumentParser(
        description="Converts WorldEdit schematic files to Minecraft Structure files."
    )
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        required=True,
        help="Path to the input schematic file or folder.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        required=False,
        default=None,
        help="Path to the output nbt file or folder.",
    )
    parser.add_argument(
        "-f",
        "--folder",
        action="store_true",
        default=False,
        help="Whether to treat the input path as a file or a folder.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Whether to print verbose output",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.CRITICAL)

    input_files, output_files = process_paths(args)

    process_files(input_files, output_files)


if __name__ == "__main__":
    main()
