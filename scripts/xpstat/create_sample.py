import cProfile


class aa:

    def hoo(self):
        print(1)
        print(2)


def add_(a, b):
    return a + b


def runit_a():
    for i in range(100):
        s = add_(i, i)
        runit_b()
    print(s)


def runit_b():
    for i in range(100):
        s = add_(i, i)
    print(s)


def main():
    runit_a()
    runit_b()


if __name__ == "__main__":
    with cProfile.Profile() as pr:
        main()
        pr.dump_stats("dump.prof")
