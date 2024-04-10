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

def merge (file_1_name, file_2_name): 
    ##  дописать, как конкретно файлы соединяются 


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

    
    
    

    clear_tmp(tmp_folder)
    