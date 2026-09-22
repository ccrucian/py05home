import typing
import abc


class TestInvalid(Exception):
    pass


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.data: list[tuple[int, str]] = []
        self.rank: int = 0
  
    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.data:
            raise IndexError("No data available to extract.")
        return self.data.pop(0)


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
        print(f"Trying to validate input '{data}': {valid}")
        return valid

    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise TestInvalid("Improper numeric data")
        if isinstance(data, (int, float)):
            self.data.append((self.rank, str(data)))
        elif isinstance(data, list):
            for i in data:
                self.data.append((self.rank, str(i)))
                self.rank += 1


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return len(data) > 0 and all(isinstance(x, str) for x in data)
        return False


class LogProcessor(DataProcessor):
    pass


def main() -> None:
    print("=== Code Nexus - Data Processor ===")

    print("Testing Numeric Processor...")
    numeri = NumericProcessor()
    numeri.validate(42)
    numeri.validate("Hello")
    string = "foo"
    values = [434, 6, 3, 4]
    print(
        f"Test invalid ingestion of string '{string}' without prior validation:")
    try:
        numeri.ingest(values)
    except TestInvalid as e:
        print(f"Got exception: {e}")
    print(f"Processing data: [{', '.join(i[1] for i in numeri.data)}]")
    print(f"Extracting {len(numeri.data)} values: ")
    while numeri.data:
        rank, value = numeri.output()
        print(f"Numeric value {rank}: {value}")


if __name__ == "__main__":
    main()
