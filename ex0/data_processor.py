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
        print(f"Trying to validate input '{data}': {valid}")
        return valid

    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise TestInvalid("Improper numeric data")
        if isinstance(data, (int, float)):
            self.processed.append((self.rank, str(data)))
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
        if isinstance(data, dict) and (
            all(isinstance(key, str)) and isinstance(value, str) 
            for key, value in data.items()):
            return True
        elif isinstance(data, list) and (all(
            isinstance(item, dict) and all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in item.items()
            )
            for item in data
        )):
            return True
        else:
            return False

    def ingest(self, data: typing.Any) -> None:
        if not self.validate(data):
            raise TestInvalid("Improper type of data")
        if isinstance(data, dict):
            for item in data:
                self.processed.append(self.rank, item)
                self.rank += 1



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
    print(f"Processing data: [{', '.join(i[1] for i in numeri.processed)}]")
    print(f"Extracting {len(numeri.processed)} values: ")
    while numeri.processed:
        rank, value = numeri.output()
        print(f"Numeric value {rank}: {value}")

    print("Testing Text Processor...")
    texting = TextProcessor()
    text_in = 42
    text_l = ["Hello", "World", "!"]
    print(f"Trying to validate input '{text_in}': {texting.validate(text_in)}")
    print(
            f"Test invalid ingestion of string '{text_in}' without prior validation:")
    try:
        texting.ingest(text_in)
    except TestInvalid as e:
        print(f"Got exception: {e}")
    print(
        f"Processing data: {text_l}"
    )
    try:
        texting.ingest(text_l)
    except TestInvalid as e:
        print(f"Got exception: {e}")
    n = 1
    print(f"Extracting {n} value...")
    for i in range(n):
        rank, text = texting.output()
        print(f"Text value {rank}: {text}")

    print("Texting Log Processor")
    logging = LogProcessor()
    strlog = "Hello"
    print(f"Trying to validate input '{strlog}': {logging.validate(strlog)}")




if __name__ == "__main__":
    main()
