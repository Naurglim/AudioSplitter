from collections import deque


class DAG():
    def __init__(self):
        self.graph = {}
    
    def add(self, node, to=None):
        if not node in self.graph:
            self.graph[node] = []
        if to:
            if not to in self.graph:
                self.graph[to] = []
            self.graph[node].append(to)
        # Add validity check.
        if len(self.sort()) != len(self.graph):
            raise Exception
    

    def in_degrees(self):
        self.degrees = {}
        for node in self.graph:
            if node not in self.degrees:
                self.degrees[node] = 0
            for pointed in self.graph[node]:
                if not pointed in self.degrees:
                    self.degrees[pointed] = 0    
                self.degrees[pointed] += 1


    def sort(self):
        self.in_degrees()
        roots = deque()
        for node in self.graph:
            if self.degrees[node] == 0:
                roots.append(node)
        
        searched = []
        while roots:
            node = roots.popleft()
            for pointer in self.graph[node]:
                self.degrees[pointer] -= 1
                if self.degrees[pointer] == 0:
                    roots.append(pointer)
            searched.append(node)
        return searched


class Pipeline():
    def __init__(self):
        self.tasks = DAG()
        
    def task(self, depends_on=None):
        def inner(f):
            self.tasks.add(f)
            if depends_on:
                self.tasks.add(depends_on, f)
            return f
        return inner
    
    def run(self, **kwargs):
        visited = self.tasks.sort()
        completed = {}
        
        for task in visited:
            for node, values in self.tasks.graph.items():
                if task in values:
                    completed[task] = task(completed[node])
            if task not in completed:
                completed[task] = task(**kwargs)
        return completed


if __name__ == "__main__":
    pipeline = Pipeline()

    @pipeline.task()
    def first():
        return 20

    @pipeline.task(depends_on=first)
    def second(x):
        return x * 2

    @pipeline.task(depends_on=second)
    def third(x):
        return x // 3

    @pipeline.task(depends_on=second)
    def fourth(x):
        return x // 4

    outputs = pipeline.run()
    print(outputs)