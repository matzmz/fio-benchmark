#!/usr/bin/python
import os
import re
import json
import matplotlib.pyplot as plt
import argparse
from datatable import dt, f, by, min, max


class Dataset:

    def __init__(self):
        self.__columns = ['threads', 'operation', 'bs', 'metric', 'value']
        self.data = dt.Frame(names=self.__columns)

    def append(self, threads, operation, bs, metric, value):
        tmp = dt.Frame([(threads, operation, int(bs), metric, float(value))],
                       names=self.__columns)
        self.data.rbind(tmp)

    def merge(self, rx_dataset):
        self.data.rbind(rx_dataset.data)

    def get_operations(self):
        return sorted(set(self.data['operation'].to_list()[0]))

    def get_metrics(self, non_empty=True):
        grouped_dataset = self.data[:, {"min": min(f["value"]), "max": max(f["value"])}, by('metric')]

        metrics = set()
        for row in grouped_dataset.to_tuples():
            if non_empty and (row[1] == 0 and row[2] == 0):
                continue
            metrics.add(row[0])
        return sorted(metrics)

    def get_byte_sizes(self):
        return sorted(set(self.data['bs'].to_list()[0]))

    def filter_by(self, field, value):
        if field not in self.__columns:
            raise Exception('invalid column')

        selected_dataset = Dataset()
        selected_dataset.data = self.data[f[field] == value, :]

        return selected_dataset

    def get_columns(self, columns=[]):
        return self.data[:, columns].to_list()


class FIORawReader:

    def __init__(self, group_prefix="thread"):
        self.__groups = {}
        self.__groups_summary = {}
        self.__group_prefix = group_prefix

    @staticmethod
    def __bandwidth_conversion(line):
        bandwidth = (line.split(','))[1].split(' ')[1].split('=')[1]
        if 'Mi' in bandwidth:
            bandwidth = bandwidth.split('M')[0]
        elif 'Ki' in bandwidth:
            bandwidth = int(bandwidth.split('K')[0]) / 2 ** 10
        elif 'Gi' in bandwidth:
            bandwidth = int(bandwidth.split('K')[0]) * 2 ** 20
        elif 'Ti' in bandwidth:
            bandwidth = int(bandwidth.split('K')[0]) * 2 ** 30
        return bandwidth

    @staticmethod
    def __io_conversion(line):
        io = line.split(',')[0].split('=')[1]
        if 'k' in io:
            io = int(float(io[0:-1]) * 10 ** 3)
        elif 'm' in io:
            io = int(float(io[0:-1]) * 10 ** 6)
        return io

    @staticmethod
    def __lat_conversion(line):
        lat = (float(line.split(',')[2].split('=')[1]))
        if line[5] == 'u':
            lat = lat / 10 ** 3
        elif line[5] == 'n':
            lat = lat / 10 ** 6
        elif line[5] == 's':
            lat = lat * 10 ** 3
        return lat

    @staticmethod
    def __single_or_multi_job(content):
        for line in content:
            if 'All clients:' in line:
                return True
        return False

    @staticmethod
    def __extract_content(content):
        multihost = FIORawReader.__single_or_multi_job(content)
        parsed_content = {}

        if multihost:
            begin_search = False
        else:
            begin_search = True

        for line in content:
            line = line.strip()

            if multihost:
                if 'All clients:' in line:
                    begin_search = True

            if begin_search:
                if 'read: IOP' in line:
                    parsed_content['read_iop'] = FIORawReader.__io_conversion(line)
                    parsed_content['read_bw'] = FIORawReader.__bandwidth_conversion(line)
                if 'read_iop' in parsed_content.keys():
                    if line[0:3] == 'lat' and 'read_lat' not in parsed_content.keys():
                        parsed_content['read_lat'] = FIORawReader.__lat_conversion(line)
                if 'write: IOP' in line:
                    parsed_content['write_iop'] = FIORawReader.__io_conversion(line)
                    parsed_content['write_bw'] = FIORawReader.__bandwidth_conversion(line)
                if 'write_iop' in parsed_content.keys():
                    if line[0:3] == 'lat' and 'write_lat' not in parsed_content.keys():
                        parsed_content['write_lat'] = FIORawReader.__lat_conversion(line)

            # Exit search here
            if begin_search and 'IO depths' in line:
                return parsed_content
        return parsed_content

    @staticmethod
    def __split_groups(self, file_content=[]):
        groups = {}
        groups_summary = {}
        groups_id_map = {}
        current_key = None
        current_group = None

        # extract group info
        for line in file_content:
            match = re.match(rf"{self.__group_prefix}s?_(\d+):\s+\(groupid=(\d+),.*\)", line)
            if match:
                current_key = match.group(1)
                current_group = match.group(2)
                groups_id_map[current_group] = current_key
                groups[current_key] = []

            match = re.match(r"^\s*\n\s*$", line)
            if match:
                current_key = None

            if current_key is not None:
                groups[current_key].append(line)

        # extract group status
        for line in self.__file_content:
            match = re.match(r"Run status group (\d+)", line)
            if match:
                current_group = match.group(1)
                current_key = groups_id_map[current_group]
                continue
            if current_group is not None:
                groups_summary[current_key] = line

        self.__groups = groups
        self.__groups_summary = groups_summary

    def parse(self, filename, operation, block_size):
        import_dt = Dataset()
        with open(filename, 'r') as fh:
            self.__split_group(fh.readlines())
            for group_name, group_values in self.__groups.items():
                parsed_content = FIORawReader.__extract_content(group_values)
                for metric, value in parsed_content.items():
                    import_dt.append(threads=group_name,
                                     operation=operation,
                                     bs=block_size,
                                     metric=metric,
                                     value=value)
        return import_dt


class FIOJsonReader:

    def __init__(self, group_prefix="thread"):
        self.__groups = {}
        self.__groups_summary = {}
        self.__group_prefix = group_prefix

    def parse(self, filename, operation, block_size):
        import_dt = Dataset()

        with open(filename, 'r') as fh:
            content = json.load(fh)
            parsed_operation = content["global options"].get("rw", "unknown")
            parsed_block_size = content["global options"].get("bs", "0").replace("k", "")

            if operation != parsed_operation:
                raise Exception(f"wrong operation (value: {parsed_operation}, expected: {operation}")

            if block_size != parsed_block_size:
                raise Exception(f"wrong operation (value: {parsed_block_size}, expected: {block_size}")

            for job in content["jobs"]:
                job_name = job["jobname"]

                match_threads = re.match(rf"{self.__group_prefix}s?_(\d+)", job_name)

                if match_threads:
                    threads = int(match_threads.group(1))
                else:
                    threads = 0

                for type in ["write", "read"]:
                    # iops
                    value = job[type][f"iops_mean"]
                    import_dt.append(threads, operation, block_size, f"{type}_iops", value)
                    # bw
                    value = job[type][f"bw"] / 1024
                    import_dt.append(threads, operation, block_size, f"{type}_bw", value)
                    # lat
                    value = job[type]["lat_ns"]["mean"] / 10 ** 6
                    import_dt.append(threads, operation, block_size, f"{type}_lat", value)

            return import_dt


def plot_operation(dataset, operation, figure):
    operation_dt = dataset.filter_by('operation', operation)

    metrics = operation_dt.get_metrics()

    figure.suptitle(operation, fontsize=14)

    axs = figure.subplots(len(metrics), 1, sharex=True)

    for idx, metric in enumerate(metrics):

        ax = axs[idx]

        metric_dt = operation_dt.filter_by('metric', metric)

        byte_sizes = metric_dt.get_byte_sizes()

        x_label = "threads"

        if "bw" in metric:
            y_label = "MB/s"
        elif "lat" in metric:
            y_label = "ns"
        elif "iop" in metric:
            y_label = "iops"
        else:
            y_label = ""

        for block_size in byte_sizes:
            current_dt = metric_dt.filter_by('bs', block_size)

            x, y = current_dt.get_columns(['threads', 'value'])

            ax.plot(x, y, label="bs: {}k".format(block_size))
            ax.set_xlabel(x_label)
            ax.set_ylabel(y_label)
            ax.set_title(metric)
            ax.set_xticks(x)
            ax.set_xticklabels(x)

        ax.legend(loc="upper right")


def plot_data(dataset, operations=[]):
    fig = plt.figure()

    operations = sorted(set(operations).intersection(set(dataset.get_operations())))

    sub_figs = fig.subfigures(1, len(operations), wspace=0.05)

    for idx, operation in enumerate(operations):
        plot_operation(dataset, operation, sub_figs[idx])

    return fig


def cli_parser():
    parser = argparse.ArgumentParser(prog='bench_plot.py', description='Benchmark FIO Plotter')

    parser.add_argument('--group-prefix-name', help='fio job group-name prefix', default='thread')

    parser.add_argument('--create-image', help="create plot image", action=argparse.BooleanOptionalAction,
                        default=True)

    parser.add_argument('--show-plot', help="create plot image", action="store_true",
                        default=False)

    parser.add_argument('result_dir', metavar='RESULT_DIR', help='result folder path')

    return parser.parse_args()


if __name__ == "__main__":

    options = cli_parser()

    result_files = os.listdir(options.result_dir)

    dataset = Dataset()

    jsonReader = FIOJsonReader(options.group_prefix_name)
    rawReader = FIORawReader(options.group_prefix_name)

    for result in result_files:
        result_path = os.path.join(options.result_dir, result)

        match = re.match(r"([a-z]+)\.(\d+)k\.result", result)
        if match is None:
            continue

        test_name = match.group(1)
        bs = match.group(2)

        if result.lower().endswith('.json'):
            result_dataset = jsonReader.parse(result_path, test_name, bs)
        else:
            result_dataset = rawReader.parse(result_path, test_name, bs)

        dataset.merge(result_dataset)

    figure = plot_data(dataset, operations=["randread", "read", "randwrite", "write"])

    if options.create_image:
        figure.set_size_inches(16, 12)
        figure.savefig("plot.png", dpi=100)

    if options.show_plot:
        plt.show()
