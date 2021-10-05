query_history = []


def get_last_few_queries(n_rows=10):

    last_n_rows = query_history[-n_rows:]
    # to make the recent query top (reversing)
    return last_n_rows[::-1]
