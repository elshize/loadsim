from random import randint
import sys

# generates params for different simulations
with open("sample.param", "r") as infile:
    s_map = {}
    for line in infile:
        key, value = line.strip().split('=')
        if key == 'queueOrder': value = sys.argv[1]
        s_map[key] = value
    s_map["randomSeed"] = sys.argv[2]

for key, value in s_map.items():
    print(key+'='+value)