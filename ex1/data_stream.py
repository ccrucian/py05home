import typing
import abc


class TestInvalid(Exception):
    pass


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.processed: list[tuple[int, str]] = []
        self.rank: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.processed:
            raise IndexError("No data available to extract.")
        return self.processed.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        valid = False
        if isinstance(data, bool):
            valid = False
        elif isinstance(data, (int, float)):
            valid = True
        elif isinstance(data, list):
            if data and all(
                (isinstance(dato, (int, float))) and not isinstance(
                        dato, bool
                    ) for dato in data
                            ):
                valid = True
        return valid

    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise TestInvalid("Improper numeric data")
        if isinstance(data, (int, float)):
            self.processed.append((self.rank, str(data)))
            self.rank += 1
        elif isinstance(data, list):
            for i in data:
                self.processed.append((self.rank, str(i)))
                self.rank += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return len(data) > 0 and all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise TestInvalid("Improper type of data")
        if isinstance(data, str):
            self.processed.append((self.rank, data))
            self.rank += 1
        else:
            for dato in data:
                self.processed.append((self.rank, dato))
                self.rank += 1


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return (
                bool(data)
                and "log_level" in data
                and "log_message" in data
                and all(
                    isinstance(key, str) and isinstance(value, str)
                    for key, value in data.items()
                )
            )
        if isinstance(data, list):
            return (
                bool(data)
                and all(
                    isinstance(item, dict)
                    and "log_level" in item
                    and "log_message" in item
                    and all(
                        isinstance(key, str) and isinstance(value, str)
                        for key, value in item.items()
                    )
                    for item in data
                )
            )
        else:
            return False

    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise TestInvalid("Improper type of data")
        if isinstance(data, dict):
            log = f"{data['log_level']}: {data['log_message']}"
            self.processed.append((self.rank, log))
            self.rank += 1
        elif isinstance(data, list):
            for item in data:
                log = f"{item['log_level']}: {item['log_message']}"
                self.processed.append((self.rank, log))
                self.rank += 1


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)
        print(f"Registering {type(proc).__name__}")

    def process_stream(self, stream: list[typing.Any]) -> None:

        for element in stream:
            found = False
            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    found = True
                    break
            if not found:
                print(
                    "Data stram error: - "
                    f"Can't process element in stream: {element} "
                )

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return

        for processor in self.processors:
            print(f"{type(processor).__name__}: "
                  f"total {processor.rank} items processed,"
                  f" remainig {len(processor.processed)}"
                  )


def main() -> None:
    print("=== Code Nexus - Data Stream ===")

    x = NumericProcessor()
    y = TextProcessor()
    z = LogProcessor()
    obj = DataStream()
    stream = [
        'Hello world', [3.14, -1, 2.71],
        [{'log_level': 'WARNING',
         'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil isconnected'}], 42,
        ['Hi', 'five']]
    obj.register_processor(x)
    obj.process_stream(stream)
    obj.print_processors_stats()

    print("Registering other data processors\nSend the same batch again")
    obj.register_processor(y)
    obj.register_processor(z)
    obj.process_stream(stream)
    obj.print_processors_stats()

    print("Consume some elements from the data processors:")

    for i in range(3):
        x.output()

    for i in range(2):
        y.output()

    for i in range(1):
        z.output()

    obj.print_processors_stats()


if __name__ == "__main__":
    main()
