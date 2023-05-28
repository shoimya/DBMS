import string



correct_tokens = [
    "INSERT",
    "INTO",
    "instructors",
    "VALUES",
    "(",
    "James",
    ",",
    29,
    ",",
    17.5,
    ",",
    None,
    ")",
    ";"
]

def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]

#remove digits and floats
def remove_digits(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + string.digits + ".")
    if word == "NULL":
        tokens.append(None)
    else:
        # if word.isdigit():
        #     tokens.append(int(word))
        # else:
        #     tokens.append(float(word))
        tokens.append(word)

    return query[len(word):]

def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find("'")
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def tokenize(query):
    tokens = []
    while query:
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        #todo integers, floats, misc. query stuff (select * for example)
        if query[0].isdigit():
            query = remove_digits(query, tokens)
            continue
        if query[0] == "*":
            tokens.append(query[0])
            query = query[1:]
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens

########################################################################

query = " INSERT   INTO instructors VALUES('James', 29, 17.5, NULL);"
query2 = "SELECT * FROM instructors"


query3 = "Create Table sports(players TEXT, wage INTEGER);"
query4 = "Insert Into sports Values(messi, 1B), (CR7, 0.9B);"
query5 = "SELECT * FROM sports ORDER BY players"

r3 = tokenize(query3)
print(r3)

r4 = tokenize(query4)
print(r4)

r5 = tokenize(query5)
print(r5)