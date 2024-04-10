import os, shutil, math, time
import multiprocessing as mp

def sort_chunk(name):
    """ name - full relative name of the file. \\
    e.g., if working dir is a/sort.py and sorted file lays at a/tmp/b.bin, then function gets \"/tmp/b.bin\" """

    chunk_size = os.path.getsize(name)
    chunk = open(name, "rb")
    sorted_arr = []

    while chunk_size - chunk.tell() > 1 :
        sorted_arr.append(chunk.read(4))
    
    sorted_arr.sort()
    chunk = open(name, "wb")
    for i in sorted_arr: chunk.write(i)
    return

def clear_tmp (tmp_folder): 
    for filename in os.listdir(tmp_folder): # clearing the tmp folder
        file_path = os.path.join(tmp_folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def merge (file_1_name, file_2_name, file_merged_name): 
    ##  дописать, как конкретно файлы соединяются 
    # files can be merged while both of them are opened. 
    file_1 = open(file_1_name, 'rb')
    file_2 = open(file_2_name, 'rb')
    file_merged = open(file_merged_name, 'ab')
    result_size = (os.path.getsize(file_1_name) + os.path.getsize(file_1_name)) / 8 # result size in bytes / 4 = number len

    for _ in range(result_size): #rewriting nums one after another
        read_1 = file_1.read(4)
        read_2 = file_2.read(4)
        if read_1 <= read_2: 
            file_merged.write(read_1)
            read_2.seek(file_2.tell()-4)
        else: 
            file_merged.write(read_2)
            read_1.seek(file_1.tell()-4)
        if (file_1.size() - file_1.tell()) <= 1 or (file_2.size() - file_2.tell()) <= 1: 
            leftover_file = file_1 if (file_1.size() - file_1.tell()) <= 1 else file_2

            file_merged.write(leftover_file.read(os.path.getsize(leftover_file) - leftover_file.tell()))
            return
        




if __name__ == '__main__':
    processors_num = os.cpu_count()
    default_file_size = 1024*1024*8*8 # bits
    bit_file_size = default_file_size // 32 # number of elements per file

    file = open('example_data.bin', "rb") # open an example file
    file_size = os.path.getsize("example_data.bin")
    tmp_folder = os.path.join('tmp')

    tmpFileCounter = 0
    while file_size - file.tell() > 1 : # create and write 
        with open(os.path.join('tmp', str(tmpFileCounter) + ".bin"), "ab") as tmpFile:
            tmpFile.write(file.read(4*bit_file_size))
        tmpFileCounter += 1

    # sort_chunk(os.path.join(tmp_folder, "0.bin"))
    for j in range(math.ceil(tmpFileCounter/processors_num)):
        process_array = []
        proc_num_to_run = processors_num if (tmpFileCounter - j*processors_num >= processors_num) else (tmpFileCounter - j*processors_num)
        for i in range(proc_num_to_run): 
            process_array.append(mp.Process(target=sort_chunk, args=(os.path.join('tmp', str(i + j*processors_num) + ".bin"), )))
        for proc in process_array: proc.start()

        for t in process_array:
            t.join()

        # todo: write sorted lists merge
    
    

    clear_tmp(tmp_folder)
    