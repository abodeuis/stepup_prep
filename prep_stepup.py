import os
import json
import logging
import argparse
from tqdm import tqdm
from pathlib import Path

import cmaas_utils.io as io
from cmaas_utils.types import Legend, MapUnitType, Provenance
from cmaas_utils.logging import start_logger

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
    parser.add_argument('-k','--known_labels',
        required=True,
        help='Json containing the known labels to filter step up on')
    parser.add_argument('-o','--output',
        default='output',
        help='Name of the output directory to write generated jsons to')
    
    args = parser.parse_args()
    args.data = post_parse_data(args.data)
    return args

def saveSteupUpJson(filepath:Path, legend:Legend):
    output_json = {}
    output_json['version'] = '5.0.1'
    output_json['flags'] = {'source' : "StepUp"}
    output_json['shapes'] = []
    for map_unit in legend.features:
        map_unit.label = map_unit.label.replace(' ', '_')
        if map_unit.type == MapUnitType.UNKNOWN:
            unit_label = map_unit.label
        else:
            unit_label = map_unit.label + '_' + map_unit.type.to_str()
        output_json['shapes'].append({'label' : unit_label, 'points' : map_unit.label_bbox, 'group_id' : None, 'shape_type' : 'rectangle', 'flags' : {}})
        if map_unit.description is not None:
            unit_label = unit_label + '_desc'
            output_json['shapes'].append({'label' : unit_label, 'points' : map_unit.description, 'group_id' : None, 'shape_type' : 'rectangle', 'flags' : {}})

    with open(filepath, 'w') as f:
        json.dump(output_json, f)

def loadLegend(filepath):
    map_name = os.path.basename(os.path.splitext(filepath)[0])
    return Legend.parse_file(filepath), map_name

def parallelLoadLegends(data, threads=32) -> dict[Legend]:
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=threads) as executor:
        legends = {}
        futures = []
        for filepath in data:
            futures.append(executor.submit(loadLegend, filepath))
        for future in futures:
            legend, map_name = future.result()
            legends[map_name] = legend

    return legends

def main(args):
    log = start_logger(
        logger_name='StepUp_Prep', 
        filepath='logs/prep_stepup.log', 
        log_level=logging.DEBUG, 
        console_log_level=logging.DEBUG, 
        writemode='w')

    os.makedirs(args.output, exist_ok=True)

    log.info(f'Loading known labels from {args.known_labels}')
    with open(args.known_labels, 'r') as fh:
        true_labels = json.loads(fh.read()) 

    log.debug(f'True dictionary contains {len(true_labels["Points"])} points, {len(true_labels["Lines"])} lines, {len(true_labels["Polygons"])} polygons, and {len(true_labels["Unknown"])} unknowns')

    log.info('Loading StepUp legends')
    stepup_legends = parallelLoadLegends(args.data)
    log.info(f'Loaded {len(stepup_legends)} legends')

    output_dict = {}
    stat_dict = {'Points' : [], 'Lines' : [], 'Likely_Lines' : [], 'Polygons' : [], 'Likely_Polygons' : [], 'Unknown' : []}
    for mapname, legend in tqdm(stepup_legends.items()):
        output_legend = Legend(provenance=Provenance(name='label_me', version='0.0.2'))
        for feature in legend.features:
            if feature.label == '':
                feature.label = feature.abbreviation
            feature.type = MapUnitType.UNKNOWN
            # # Skip feature if already assigned a type
            # if feature.type != MapUnitType.UNKNOWN:
            #     continue
            # Check if label is a direct match to a known label
            label = feature.label.lower()
            if label in true_labels['Points']:
                stat_dict['Points'].append(label)
                feature.type = MapUnitType.POINT
                output_legend.features.append(feature)
            elif label in true_labels['Lines']:        
                stat_dict['Lines'].append(label)
                feature.type = MapUnitType.LINE
                output_legend.features.append(feature)
            elif label in true_labels['Polygons']:
                stat_dict['Polygons'].append(label)
                feature.type = MapUnitType.POLYGON
                output_legend.features.append(feature)
            else:
                # Check if label is less then 5 characters long (probably a poly abbreviation)
                if len(feature.label) < 5:
                    stat_dict['Likely_Polygons'].append(label)
                    feature.type = MapUnitType.POLYGON
                    output_legend.features.append(feature)
                    continue
                else:
                    # Check 
                    line_found = False
                    for line in true_labels['Lines']:
                        if line in label:
                            stat_dict['Likely_Lines'].append(line)
                            feature.type = MapUnitType.LINE
                            output_legend.features.append(feature)
                            line_found = True
                            break
                    if not line_found:
                        stat_dict['Unknown'].append(label)
                        feature.type = MapUnitType.UNKNOWN
                        output_legend.features.append(feature)
        output_dict[mapname] = output_legend

    log.info(f'Test dictionary contains {len(stat_dict["Points"])} points, {len(stat_dict["Lines"])} lines, {len(stat_dict["Likely_Lines"])} likely lines, {len(stat_dict["Polygons"])} polygons, {len(stat_dict["Likely_Polygons"])} likely polygons, and {len(stat_dict["Unknown"])} unknowns')
    log.info(f'Test dictionary contains {len(set(stat_dict["Points"]))} unique points, {len(set(stat_dict["Lines"]))} unique lines, {len(set(stat_dict["Likely_Lines"]))} unique likely lines, {len(set(stat_dict["Polygons"]))} unique polygons, {len(set(stat_dict["Likely_Polygons"]))} unique likely polygons, and {len(set(stat_dict["Unknown"]))} unique unknowns')

    os.makedirs(args.output, exist_ok=True)
    for mapname, legend in tqdm(output_dict.items()):
        saveSteupUpJson(os.path.join(args.output, mapname + '.json'), legend)


if __name__=='__main__':
    args = parse_command_line()
    main(args)