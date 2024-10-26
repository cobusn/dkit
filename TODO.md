# Busy
* upgrade to mistune 2
* improve test coverage

# Next
* Handle Dicts and List data types 
* Upgrade to sqlalchemy 2
* validations on parsing Decimal(precision=X, scale=Y), will currently parse
  without defaults or errors;
* AWS S3 integration (s3fs..)
* support for autolink in Reportlab Renderer
* generate report folder structure and configuration files (via dk build (init-tex | init-rl))

# Backlog 
* type guesser guesses numpy ints as string (should it generate an error)
* fix python/cannonical type conversion (e.g. python float must be double)
* refactor etl.writer
* factor out Cerberus for a more modern replacement
* update for Python 3.10
* optimise imports
* reportlab: document structure added to pdf 
* table of contents for Reportlab documents
* change major imports to lazy loading
* integrate plantuml for documentation?
* data analyser feature (automated data analysis with report output)
* review pandoc templates for anything useful: https://github.com/Wandmalfarbe/pandoc-latex-template/tree/master/examples
* https://julien.danjou.info/finding-definitions-from-a-source-file-and-a-line-number-in-python/
* protobuf integration
* apache parquet integration 
* pgfplotstable http://ftp.sun.ac.za/ftp/CTAN/graphics/pgf/contrib/pgfplots/doc/pgfplotstable.pdf
* allow options to be passed for opening a network database connection. the options should be stored in the connection settings..
* optimise mpak schema to use integers/floats for storing dates
* optimise plots to define grammar and then apply data at plot instead of defining the data upfront.


# validation using dataclasses
s it possible to use python DataClasses to validate data from a dictionary using type data from the dataclass de
finition?
is it possible to use python DataClasses to validate data from a dictionary using type data from the dataclass definition?
Yes, it is possible to use Python's data classes to validate data from a dictionary, but it requires additional steps since data classes alone do not perform validation or type checking at runtime by default. However, you can use libraries or write custom validation logic to achieve this.

Here's a simple way to perform basic type validation when using data classes:

1. **Use Type Hints and a Custom Constructor:**

   You can write a custom method to validate the types of data when instantiating the dataclass from a dictionary.

2. **Use Libraries for Enhanced Validation:**

   Libraries such as `pydantic` give you more robust type-checking and validation capabilities.

Here's an example using both approaches:

### Custom Constructor for Validation

```python
from dataclasses import dataclass, fields
from typing import Any, get_type_hints

@dataclass
class Person:
    name: str
    age: int

    @classmethod
    def from_dict(cls, data: dict):
        # Perform type checking
        type_hints = get_type_hints(cls)
        for key, value in data.items():
            if key in type_hints and not isinstance(value, type_hints[key]):
                raise TypeError(f"Expected type {type_hints[key]} for field '{key}', got {type(value)}")

        return cls(**data)

# Example usage
data = {'name': 'John', 'age': 30}
person = Person.from_dict(data)
print(person)  # Person(name='John', age=30)
```

