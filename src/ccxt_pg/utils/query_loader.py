from pathlib import Path


class QueryLoader:
    def __init__(self, query_dir=None):
        if query_dir is None:
            self.query_dir = Path(__file__).parent / "queries"

        self.queries = {}

        # Load queries at initialization

        self.load_queries()

    def load_queries(self):
        """Load all SQL queries from the query directory."""

        for query in self.query_dir.glob("*.sql"):
            print(query.stem)
            with open(query, "r") as f:
                self.queries[query.stem] = f.read()

    def get_query(self, query_name):
        """Retrieve a query by its name."""
        return self.queries.get(query_name, None)
