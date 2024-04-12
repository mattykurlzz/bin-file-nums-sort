import os, shutil, math  # importing libraries
import multiprocessing as mp


def sort_chunk(name):  # function for sorting a chunk-file
    """ name - full relative name of the file. \\
    e.g., if working dir is a/sort.py and sorted file lays at a/tmp/b.bin, then function gets \"/tmp/b.bin\" """

    chunk_size = os.path.getsize(
        name
    )  # a variable to store size of the chunk file if bytes
    chunk = open(name, "rb")  # read-opened chunk in byte mode
    sorted_arr = []  # an array for 4-byte long numbers to sort

    while (
        chunk_size - chunk.tell() > 1
    ):  # unless we reach the end of a file, we read 4 bytes from it and append it to an array
        sorted_arr.append(chunk.read(4))

    sorted_arr.sort()  # sorting an array in ascending order
    chunk = open(
        name, "wb"
    )  # reopen a file in write mode. when the file is opened that way, it gets cleared
    for i in sorted_arr:  # for every element in array we write it to the chunk-file
        chunk.write(i)
    return


def clear_tmp(tmp_folder="tmp", filenames=[]):
    """a function for clearing tmp-folder.

    If tmp_folder argument is passed (or no args), clears the whole folder

    If filenames array is passed - clears only passed files.
    Filenames should contain their full relative name"""
    if len(filenames) == 0:
        for filename in os.listdir(tmp_folder):  # clearing the tmp folder
            file_path = os.path.join(tmp_folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(
                    file_path
                ):  # if for some reason a directory was created in tmp, it is deleted
                    shutil.rmtree(file_path)
            except Exception as e:
                print("Failed to delete %s. Reason: %s" % (file_path, e))
    else:
        for file_path in filenames:  # clearing some files
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print("Failed to delete %s. Reason: %s" % (file_path, e))


def merge(file_1_name, file_2_name, file_merged_name):  # a function for sorted merge
    """function takes 3 positional arguments:

    file_1_name - a full relative name of a file 1 to be merged
    file_2_name - a full relative name of a file 2 to be merged
    file_merged_name - a full relative name of a resulting file

    this function takes two files, reads first 4-byte numbers and compares them. The smallest gets written to a new file.
    Then, function movers a read-cursor in the file it read from and compares the next two numbers.
    Whenever one of two files gets fully read, the leftovers of the other one are appendet to a resulting file
    """
    file_1 = open(file_1_name, "rb")
    file_2 = open(file_2_name, "rb")  # opening the chunks

    file_1_size = os.path.getsize(file_1_name)  # byte file sizes
    file_2_size = os.path.getsize(file_2_name)

    file_merged = open(
        file_merged_name, "wb"
    )  # creating of clearing of a resulting file
    result_size = (
        file_1_size + file_2_size
    ) // 4  # result size in (bytes / 4) = number len

    for _ in range(
        result_size
    ):  # this loop prevents from trying to read outside the file boundaries
        read_1 = file_1.read(
            4
        )  # reading both file 32-bit numbers. this function shifts corsors to the right
        read_2 = file_2.read(4)
        if read_1 <= read_2:  # comparing 1st num to the 2nd
            file_merged.write(read_1)  # write the smallest
            file_2.seek(
                file_2.tell() - 4
            )  # for the bigger number, it's file-parrent shifts it's cursor back to the left to then read that number again
        else:
            file_merged.write(read_2)
            file_1.seek(file_1.tell() - 4)

        if (file_1_size - file_1.tell()) <= 1 or (
            file_2_size - file_2.tell()
        ) <= 1:  # checking if anu file's cursor have reached the end of a file
            leftover_file = (
                file_1 if (file_1_size - file_1.tell()) > 0 else file_2
            )  # l\o file gets chosen dased on which one had reached the end
            leftover_file_size = (
                file_1_size if leftover_file == file_1 else file_2_size
            )  # same for file size

            file_merged.write(
                leftover_file.read(leftover_file_size - leftover_file.tell())
            )  # merged file appends everything that is left over in a l\o file
            return


if __name__ == "__main__":  # main body

    processors_num = (
        os.cpu_count() * 4
    )  # number of used pricessors. As much as I know, the more there is, the faster the program'll go
    # but in this case the more processes run simultaneously, the more RAM is used, so it is easier to set them relatively to physical CPUs
    default_file_size = (
        128 * 8
    )  # bit file size of the smallest (0-) chunk. Ideally, should be getting availale RAM and dividing it by processors_num
    # but I cannot overcome virtualized memory abstraction yet
    bit_file_size = default_file_size // 32  # number of 32-bit nums per chunk

    file = open("example_data.bin", "rb")  # open an example file
    file_size = os.path.getsize("example_data.bin")  # size of a file
    tmp_folder = os.path.join("tmp")  # setting up a tmp directory

    if not os.path.exists(tmp_folder):  # if there is no tmp folder, it gets created
        os.makedirs(tmp_folder)
    else:
        clear_tmp(tmp_folder=tmp_folder)

    tmp_file_counter = 0
    while file_size - file.tell() > 1:  # create and write
        write_file_name = os.path.join(
            "tmp", "0-" + str(tmp_file_counter) + ".bin"
        )  # every file in tmp directory should have
        # paricular name based on their mere iteration and sequential number. '0-' means the file is at 0th level of merge and file counter is
        # used to set it's number
        with open(write_file_name, "wb") as tmpFile:
            tmpFile.write(file.read(4 * bit_file_size))
        tmp_file_counter += 1

    for j in range(math.ceil(tmp_file_counter / processors_num)):
        process_array = []  # an array of processes to launch
        proc_num_to_run = (
            processors_num
            if (tmp_file_counter - j * processors_num >= processors_num)
            else (tmp_file_counter - j * processors_num)
        )  # number of processes is decided depending on how much files there are yet to sort. If there are more files than available processes, then al of them sould be loaded, else only as much as there are files
        for i in range(proc_num_to_run):  # set up procs to run
            process_array.append(
                mp.Process(
                    target=sort_chunk,
                    args=(
                        os.path.join(
                            "tmp", "0-" + str(i + j * processors_num) + ".bin"
                        ),
                    ),
                )
            )
        for proc in process_array:
            proc.start()  # run the procs

        for t in process_array:  # wait 'till procs are complete and merged
            t.join()

    merges_expected = math.ceil(
        math.log2(len(os.listdir(tmp_folder)))
    )  # number of exbected merges to get one complete file
    for k in range(merges_expected):
        tmp_len = len(
            os.listdir(tmp_folder)
        )  # current length of a folder represents a number of files to be merged
        for j in range(math.ceil(tmp_len / processors_num / 2)):
            proc_num_to_run = (
                processors_num
                if (tmp_len - j * processors_num * 2 > processors_num * 2)
                else math.ceil((tmp_len - j * processors_num * 2) / 2)
            )
            process_array = []
            files_clear = []

            for i in range(proc_num_to_run):  # repeat until all processors are busy
                if (
                    tmp_len - j * processors_num * 2 - i * 2 > 1
                ):  # if there is more than one file left, then the next two get merged together
                    file_1_name = os.path.join(
                        "tmp",
                        str(k) + "-" + str(i * 2 + j * processors_num * 2) + ".bin",
                    )  # algorithm to find a file by name
                    file_2_name = os.path.join(
                        "tmp",
                        str(k) + "-" + str(i * 2 + j * processors_num * 2 + 1) + ".bin",
                    )
                    res_file_name = os.path.join(
                        "tmp", str(k + 1) + "-" + str(i + j * processors_num) + ".bin"
                    )  # merged-chunks have name format of type 'tmp/a-b.tmp' where a is an iteration of merging and b is a number of file merged on that iter

                    process_array.append(
                        mp.Process(
                            target=merge,
                            args=(
                                file_1_name,
                                file_2_name,
                                res_file_name,
                            ),
                        )
                    )

                    files_clear.append(
                        file_1_name
                    )  # appending source files to clear list to clear them after merging
                    files_clear.append(file_2_name)
                elif (
                    tmp_len - j * processors_num * 2 - i * 2 == 1
                ):  # only one file is left to merge, so we just rename it instead of merging
                    file_1_name = os.path.join(
                        "tmp",
                        str(k) + "-" + str(i * 2 + j * processors_num * 2) + ".bin",
                    )
                    file_1_new_name = os.path.join(
                        "tmp", str(k + 1) + "-" + str(i + j * processors_num) + ".bin"
                    )
                    os.rename(file_1_name, file_1_new_name)
            for proc in process_array:
                proc.start()
            for proc in process_array:
                proc.join()
            if len(files_clear) != 0:
                clear_tmp(filenames=files_clear)

    if os.path.isfile("sorted.bin"):  # in case if a sorted file exists, it is deleted
        clear_tmp(filenames=["sorted.bin"])
    os.rename(
        os.path.join(tmp_folder, os.listdir(tmp_folder)[0]), "sorted.bin"
    )  # resulting merged file (the only one left in tmp folder) gets renamed to 'sorted.bin'

    # clear_tmp(tmp_folder=tmp_folder) # clear the tmp folder. left there for debugging
