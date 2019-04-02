import hashlib
import json
import statistics
import sys
from datetime import datetime
from decimal import Decimal

from pyjarowinkler import distance

SIMILAR_THRESHOLD = 0.85
TIMING_MAX_DEVIATION_DAYS = 3  # max deviation in transactions intervals
TIMING_MIN_DAYS = 4  # minimum distance between two transactions


class Transaction(object):
    """
    Class representing a transaction.
    """
    def __init__(self, date, description, amount):
        self.uuid = hashlib.md5('{}'.format(date + description + str(amount)).encode()).hexdigest()
        self.date = datetime.strptime(date, '%m/%d/%Y')
        self.description = description
        self.amount = Decimal(str(amount))  # more exact than floating point (eg. 1.3)

    def __repr__(self):
        return f'{self.date.strftime("%m/%d/%Y"), self.description}'


class Sequence(object):
    """
    Class representing a sequence of transactions. Transactions are stored in a dictionary for fast
    retrieval.
    """
    def __init__(self, transactions, frequency):
        self.transactions = {}
        if transactions is not None:
            for t in transactions:
                self.transactions[t.uuid] = t
        self.frequency = frequency

    def get_other_transactions(self, transaction):
        """
        Return all other transactions that part of the sequence besides `transaction` it self, if
        any.
        """
        try:
            t_uuid = transaction.uuid
            self.transactions[t_uuid]
            return [self.transactions[k] for k in self.transactions.keys() if k != t_uuid]
        except KeyError:
            return []


class SequenceFinder(object):

    def __init__(self):
        self.transactions = {}
        self.clusters = []
        self.distances = {}
        self.sequences = []
        self.sequences_map = {}

    @staticmethod
    def get_pair_key(t1, t2):
        sorted_transactions = sorted([t1, t2], key=lambda t: t.uuid)
        pair_key = '{}|{}'.format(sorted_transactions[0].uuid, sorted_transactions[1].uuid)
        return pair_key

    @staticmethod
    def get_mean_interval(transactions):
        """
        Calculate the mean of the intervals duration of the given `transactions`, in seconds.
        """
        # first calculate mean of transaction intervals
        intervals = []
        prev_t = None
        for t in transactions:
            if prev_t is not None:
                intervals.append((t.date - prev_t.date).days)
            prev_t = t
        return statistics.mean(intervals)

    def get_transactions_from_key(self, pair_key):
        uuid1, uuid2 = pair_key.split('|')
        return self.transactions[uuid1], self.transactions[uuid2]

    def get_distance(self, t1, t2):
        return self.distances[self.get_pair_key(t1, t2)]

    def store_sequence(self, transactions, frequency):
        """
        Store the `transactions` as a `Sequence` object along with the `frequency`. Also updates
        `self.sequences_map` for easy access.
        """
        seq = Sequence(transactions, frequency)
        self.sequences.append(seq)
        self.sequences_map.update({
            t.uuid: seq for t in transactions
        })

    def get_rest_of_sequence(self, transaction):
        """
        Return all other transactions (in order) that are part of the sequence that `transaction`
        belongs to.
        """
        rest_of_sequence = None
        try:
            sequence = self.sequences_map[transaction.uuid]
            rest_of_sequence = sequence.get_other_transactions(transaction)
        except KeyError:
            pass
        return rest_of_sequence

    def load_data(self, filename):
        """
        Load transactions data from `self.filename` into `self.transactions` as `Transaction`
        instances.
        """
        with open(filename) as json_file:
            file_data = json.load(json_file)
            for data in file_data:
                transaction = Transaction(data['date'], data['description'], data['amount'])
                self.transactions[transaction.uuid] = transaction

    def add_to_clusters(self, transaction):
        """
        Add `transaction` and `t2` to existing clusters (if their similarity is enough) or create new
        cluster(s) for them, using the `distances` dict.
        """
        add_alone = True
        for cluster in self.clusters:
            if transaction in cluster:
                add_alone = False
                break
            else:
                add_to_cluster = True
                for t in cluster:
                    jw_distance = self.get_distance(transaction, t)
                    if jw_distance < SIMILAR_THRESHOLD:
                        add_to_cluster = False
                        break
                if add_to_cluster:  # add to cluster is similar to all elements
                    cluster.add(transaction)
                    add_alone = False
        if add_alone:
            # no similar cluster, create new
            self.clusters.append(set([transaction]))

    def calculate_distances(self, filename=None):
        """
        Calculate the distance for every (unique) pair of transactions.
        """
        if filename is not None:
            sys.stdout.write('Loading distances from file...')
            sys.stdout.flush()
            with open(filename) as json_file:
                self.distances = json.load(json_file)
            sys.stdout.write('\rLoading distances from file... DONE\n')
            sys.stdout.flush()
        else:
            print('Calculating Jaro-Winkler distances for descriptions...')
            transactions = self.transactions.values()
            done = 0
            for t1 in transactions:
                for t2 in transactions:
                    percent_calculated = int(done * 100 / len(transactions)**2)
                    sys.stdout.write('\rCalculated {}%'.format(percent_calculated))
                    sys.stdout.flush()
                    if t1.uuid == t2.uuid:
                        continue

                    pair_key = self.get_pair_key(t1, t2)
                    try:
                        self.distances[pair_key]
                    except KeyError:
                        # new pair, calculate distance and store as unique
                        jw_distance = distance.get_jaro_distance(
                            t1.description, t2.description, winkler=True, scaling=0.13)
                        self.distances[pair_key] = jw_distance
                    done += 1

    def calculate_clusters(self):
        """
        Calculate the clusters of `Transactions` by description similarity using
        `SIMILAR_THRESHOLD` to create them.
        """
        # first calculate all distances and unique pairs
        total_done = 0
        total_pairs = len(self.distances)
        transactions_seen = {}
        for pair_key in self.distances:
            t1, t2 = self.get_transactions_from_key(pair_key)
            try:
                transactions_seen[t1.uuid]
            except KeyError:
                self.add_to_clusters(t1)
                transactions_seen[t1] = True

            try:
                transactions_seen[t2.uuid]
            except KeyError:
                self.add_to_clusters(t2)
                transactions_seen[t2] = True

            total_done += 1
            if total_done % 10 == 0:
                sys.stdout.write('\rExamining pairs... {}%'.format(
                    int(total_done * 100 / total_pairs)))
                sys.stdout.flush()

    def find_sequences(self):
        """
        Finds the sequences in each of the clusters of `self.clusters``
        """
        for cluster in self.clusters:
            if len(cluster) < 4:  # early elimination
                continue

            cluster_transactions = sorted(cluster, key=lambda t: t.date)

            tentative_sequence = []
            # calculate the minimum and maximum distance between transactions of the cluster
            cluster_mean = self.get_mean_interval(cluster_transactions)
            interval_max = cluster_mean + TIMING_MAX_DEVIATION_DAYS
            interval_min = max(cluster_mean - TIMING_MAX_DEVIATION_DAYS, TIMING_MIN_DAYS)

            idx = 0
            prev_transaction = None
            for transaction in cluster_transactions:
                if prev_transaction is None:
                    prev_transaction = transaction
                    idx += 1
                    continue

                interval = (transaction.date - prev_transaction.date).days
                if interval < interval_min or interval_max < interval:
                    # sequence broken by transaction

                    if len(tentative_sequence) >= 4:
                        # save previous found sequence and calculate new interval mean and limits
                        self.store_sequence(tentative_sequence, cluster_mean)
                        tentative_sequence = []
                        cluster_mean = self.get_mean_interval(cluster_transactions[idx:])
                        interval_max = cluster_mean + TIMING_MAX_DEVIATION_DAYS
                        interval_min = max(cluster_mean - TIMING_MAX_DEVIATION_DAYS, TIMING_MIN_DAYS)
                    else:
                        # previous tentative is not long enough, delete it
                        tentative_sequence = []
                else:
                    tentative_sequence.append(prev_transaction)

                prev_transaction = transaction
                idx += 1

            # only add if tentative sequence meets condition 3.
            if len(tentative_sequence) >= 4:
                self.store_sequence(tentative_sequence, cluster_mean)
        return self.sequences

    def print_results(self):
        print('SEQUENCES')
        number = 1
        for seq in self.sequences:
            print('\n')
            print(f'SEQUENCE #{number}')
            print(f'Frequency: ~{seq.frequency} days')
            prev_t = None
            interval = None
            for t in seq.transactions.values():
                if prev_t is not None:
                    interval = (t.date - prev_t.date).days
                print('{} \t {} \t {} \t {} days'.format(
                    t.date.strftime('%m/%d/%Y'), t.description, t.amount, interval or '-'))
                prev_t = t
            number += 1

    def run(self, transactions_file, distances_file=None):
        self.load_data(transactions_file)
        self.calculate_distances(filename=distances_file)
        self.calculate_clusters()
        self.sequences = self.find_sequences()
