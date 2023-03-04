import argparse
#import tensorflow as tf

def recreate_input_file(args):
    print(args)
    # first, we read the inputfile, in json format
    # we load it into a pandas df, where 
def main():
    # create argument parser
    parser = argparse.ArgumentParser(
        prog = 'AutoStocks Model Manager',
        description= 'Train models, reorganize data and more')
    subparsers= parser.add_subparsers()
    data_parser = subparsers.add_parser('data')
    data_parser.add_argument('inputfile', type=str, help='Main Input')
    data_parser.set_defaults(func=recreate_input_file) 
    args = parser.parse_args()
    args.func(args) 
main()

    
