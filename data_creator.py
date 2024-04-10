import random, time

example_data = 'example_data.bin'
with open(example_data, 'w') as example:
    for x in range(300000000):
        example.write(str(random.randint(1000, 9999)))
    example.close()
time.sleep(3)
with open(example_data, 'r') as example:
    print(example.read(1))