import os
import argparse
import logging
import json

import cmaas_utils.io as io
from cmaas_utils.types import MapUnitType
from cmaas_utils.logging import start_logger

# requirements
# cmaas_utils

# Known json files to scrap labels from
def parse_command_line():
    """Runs Command line argument parser for pipeline. Exit program on bad arguments. Returns struct of arguments"""
    from typing import List
    def parse_data(path: str) -> List[str]:
        """Command line argument parser for --data. --data should accept a list of file and/or directory paths as an
           input. This function is called on each individual element of that list and checks if the path is valid."""
        # Check if it exists
        if not os.path.exists(path):
            msg = f'Invalid path "{path}" specified : Path does not exist'
            raise argparse.ArgumentTypeError(msg+'\n')
        return path
    
    def post_parse_data(data : List[str]) -> List[str]:
        """Loops over all data arguments and finds all tif files. If the path is a directory expands it to all the valid
           files paths inside the dir. Returns a list of valid files. Raises an argument exception if no valid files were given"""
        data_files = []
        for path in data:
            # Check if its a directory
            if os.path.isdir(path):
                data_files.extend([os.path.join(path, f) for f in os.listdir(path) if f.endswith('.json')])
            if os.path.isfile(path) and path.endswith('.json'):
                data_files.append(path)
        if len(data_files) == 0:
            msg = 'No valid files where given to --data argument. --data should be given a path or paths to file(s) \
                    and/or directory(s) containing the ground truth json with labels. program will only run on .json files'
            raise argparse.ArgumentTypeError(msg)
        return data_files
    
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--data', 
        type=parse_data,
        required=True,
        nargs='+',
        help='Path to file(s) and/or directory(s) containing the ground truth labeled json data to generate the labels from. The \
                program will run inference on any .tif files.')   
    parser.add_argument('-o','--output',
        type=str,
        default='known_labels.json',
        help='Name of the output file to generate')
    
    args = parser.parse_args()
    args.data = post_parse_data(args.data)
    return args

def main(args):
    log = start_logger('StepUpLabelFilter', 'logs/gen_known_labels.log', log_level=logging.DEBUG, console_log_level=logging.INFO, writemode='w')
    
    log.info('Loading True labels')
    true_legends = io.parallelLoadLegends(args.data)

    # Auto-detect ground truth labels
    true_labels = {'Points' : [], 'Lines' : [], 'Polygons' : [], 'Unknown' : []}
    for legend in true_legends.values():
        for feature in legend.features:
            if feature.type == MapUnitType.POINT:
                true_labels['Points'].append(feature.label.lower())
            elif feature.type == MapUnitType.LINE:
                true_labels['Lines'].append(feature.label.lower())
            elif feature.type == MapUnitType.POLYGON:
                true_labels['Polygons'].append(feature.label.lower())
            else:
                true_labels['Unknown'].append(feature.label.lower())

    # Manual Adding of labels
    known_lines = ['monocline','syncline','anticline','antiform','synform']
    for line in known_lines:
        true_labels['Lines'].append(line)

    log.info(f'True dictionary contains {len(true_labels["Points"])} points, {len(true_labels["Lines"])} lines, {len(true_labels["Polygons"])} polygons, and {len(true_labels["Unknown"])} unknowns')
    log.info(f'True dictionary contains {len(set(true_labels["Points"]))} unique points, {len(set(true_labels["Lines"]))} unique lines, {len(set(true_labels["Polygons"]))} unique polygons, and {len(set(true_labels["Unknown"]))} unique unknowns')

    # Save unique labels
    log.info(f'Saving true labels to {args.output}')
    true_labels['Points'] = list(set(true_labels['Points']))
    true_labels['Lines'] = list(set(true_labels['Lines']))
    true_labels['Polygons'] = list(set(true_labels['Polygons']))
    true_labels['Unknown'] = list(set(true_labels['Unknown']))
    with open(args.output, 'w') as fh:
        fh.write(json.dumps(true_labels))
    
if __name__=='__main__':
    args = parse_command_line()
    main(args)
