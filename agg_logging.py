# Logging for scripts

import pprint

out_file = None

def set_output(file, heading):
    global out_file
    out_file = open(file, "w")
    out_file.write(heading + "\n")

def dprint(*args):
    if out_file != None:
        str_args = ""
        for arg in args:
            str_args += str(arg)
        out_file.write(str_args + "\n")
    else:
        print(args)

def pretty_printer():
    if out_file != None:
        return pprint.PrettyPrinter(indent=4, stream=out_file)
    else:
        return pprint.PrettyPrinter(indent=4)

def close():
    if out_file != None:
        out_file.close()

