import argparse
import subprocess
import threading
import os
import statistics
from collections import namedtuple

AggregatedSummary = namedtuple('AggregatedSummary', [
    'latency_mean_avg',
    'latency_99th_percentile_avg',
    'latency_max_stddev',
    'op_rate_sum'
])


class CommandRunner:
    def __init__(self, node_ip, duration, cassandra_threads, output_dir='results'):
        self.node_ip = node_ip
        self.duration = duration
        self.cassandra_threads = cassandra_threads
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _run_command(self, thread_number):
        # its better to pass assembled command object to this function for flexibility
        duration = self.duration[thread_number]

        output_file = os.path.join(self.output_dir, f'output_{thread_number}.txt')
        command = f"docker exec some-scylla cassandra-stress write duration={duration}s -rate " \
                  f"threads={self.cassandra_threads} -node {self.node_ip}"

        with open(output_file, 'w') as file:
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            for line in process.stdout:
                file.write(line)
            process.wait()

    def run_stress_tests(self, num_threads):
        threads = []
        for i in range(num_threads):
            t = threading.Thread(target=self._run_command, args=(i,))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()
        return [os.path.join(self.output_dir, f) for f in os.listdir(self.output_dir) if f.startswith('output_')]


class LogReader:
    def __init__(self, file_path):
        self.file_path = file_path

    def get_values(self):
        with open(self.file_path, 'r') as file:
            for line in file:
                yield line.strip()


class LogCalculator:
    def __init__(self):
        self.op_rate_sum = 0
        self.latency_mean_values = []
        self.latency_99th_percentile_values = []
        self.latency_max_values = []

    def process_line(self, line):
        parts = line.split(':', 1)
        if len(parts) < 2:
            return

        key, value = parts
        key = key.strip()
        value_tokens = value.strip().split()
        if not value_tokens:
            return

        try:
            value = float(value_tokens[0].replace(',', ''))
        except ValueError:
            return  # Skip lines where conversion to float is not possible

        # Aggregate data based on the key
        if 'Latency max' in key:
            self.latency_max_values.append(value)
        elif 'Latency 99th percentile' in key:
            self.latency_99th_percentile_values.append(value)
        elif 'Latency mean' in key:
            self.latency_mean_values.append(value)
        elif 'Op rate' in key and 'WRITE' in line:
            self.op_rate_sum += value

    def calculate_aggregations(self):
        mean_avg = statistics.mean(self.latency_mean_values) if self.latency_mean_values else 0
        percentile_avg = statistics.mean(
            self.latency_99th_percentile_values) if self.latency_99th_percentile_values else 0
        max_stddev = statistics.stdev(self.latency_max_values) if len(self.latency_max_values) > 1 else 0
        return AggregatedSummary(mean_avg, percentile_avg, max_stddev, self.op_rate_sum)


class SummaryAggregator:
    def __init__(self, log_files):
        self.log_files = log_files

    def aggregate(self):
        calculator = LogCalculator()
        for file_path in self.log_files:
            reader = LogReader(file_path)
            for line in reader.get_values():
                calculator.process_line(line)
        return calculator.calculate_aggregations()

    def print_summary(self, summary):
        print(f'Op rate sum: {summary.op_rate_sum} op/s')
        print(f'Average Latency mean: {summary.latency_mean_avg} ms')
        print(f'Average Latency 99th percentile: {summary.latency_99th_percentile_avg} ms')
        print(f'Standard deviation of Latency max: {summary.latency_max_stddev} ms')


def main():
    parser = argparse.ArgumentParser(description="Run and analyze Cassandra Stress Tests")
    parser.add_argument('--node_ip', type=str, required=True, help="Node IP to run stress command")
    parser.add_argument('--threads', type=int, default=5, help="Number of concurrent threads for the stress tests")
    parser.add_argument('--duration', type=str, required=True,
                        help="Comma-separated list of durations for each stress command in seconds")
    parser.add_argument('--cassandra_threads', type=int, default=10, help="Number of threads per Cassandra stress command")
    args = parser.parse_args()

    durations = [int(dur) for dur in args.duration.split(',')]
    if len(durations) != args.threads:
        print(f"Error: Number of durations: {len(durations)}, dont match with number of threads: {args.threads}.")
        return

    runner = CommandRunner(args.node_ip, durations, args.cassandra_threads)
    log_files = runner.run_stress_tests(args.threads)
    aggregator = SummaryAggregator(log_files)
    summary = aggregator.aggregate()
    aggregator.print_summary(summary)


if __name__ == "__main__":
    main()
