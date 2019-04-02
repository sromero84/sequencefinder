# Sequence Finder

## Install

Create a virtual env with Python3.6, activate it and run `pip install -r requirements.txt`

## Usage

The class contructor takes the `transactions` json file path and an optional `distances` json file, containing precalculated distances for the transaction descriptions.

The `distances.json` file in the respository has been calculated the Jaro-Wrinkler distance [1].

Example:

```
from finder import SequenceFinder

f = SequenceFinder()
f.run('transactions.json', distances_file='distances.json')
```

You can also print the result (sequences) running `f.print_results()`.

For any given `Transaction`, once the Sequences have been calculated, it's easy to obtain the other transactions in the sequence by running:

```
f.get_rest_of_sequence(transaction)
```


[1] https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance
