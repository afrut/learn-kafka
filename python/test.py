if __name__ == "__main__":
    with open(".\\resources\\a-scandal-in-bohemia.txt", "r", encoding="utf-8") as fl:
        lines = fl.readlines()

        for line in lines:
            print(line)
