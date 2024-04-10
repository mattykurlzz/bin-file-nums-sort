import random, time, sys

example_data = 'example_data.bin'
with open(example_data, 'w') as example:
    for x in range(10000000):
        example.write(str(random.randint(0, 999).to_bytes(2, 'little')))
    example.close()
time.sleep(3)
with open(example_data, 'r') as example:
    print(example.read(1))