from importlib.resources import open_text
import random
from typing import List
from . import document as doc

# load only once
with open_text("dkit.resources", "lorem.txt") as infile:
    word_list = infile.read().split(" ")
    random.shuffle(word_list)


class Lorem(object):

    def word(self) -> str:
        """return a random word"""
        return random.choice(word_list)

    def words(self, n) -> List[str]:
        """return a list of n random words"""
        for i in range(n):
            yield self.word()

    def txt_sentence(self, min=4, max=20) -> str:
        """return a random text sentence"""
        n = int(random.triangular(min, max))
        s = list(self.words(n))
        s[0] = s[0].capitalize()
        s[-1] = s[-1] + "."
        return " ".join(s)

    def txt_paragraph(self, min=2, max=5):
        """return a random text paragraph"""
        n = int(random.triangular(min, max))
        sentences = [self.txt_sentence() for i in range(n)]
        return " ".join(sentences)

    def paragraph(self, min=2, max=5) -> doc.Paragraph:
        """return paragraph"""
        return doc.Paragraph(self.txt_paragraph(min, max))

    def unordered_list(self, least=3, most=6) -> doc.List:
        """simple unordered list"""
        n = int(random.triangular(least, most))
        lst = doc.List(ordered=False)
        for i in range(n):
            lst.add_entry(self.txt_sentence())
        return lst
