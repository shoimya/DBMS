"""
Name: Shoimya Chowdhury
Netid: chowdh83
PID:159958342
How long did this project take you? too long a solid 24


Sources:

"""
import string
from operator import itemgetter

# CREATE TABLE
# INSERT INTO VALUES
# SELECT FROM ORDER BY
# SQL NULL = Python3 None
# SQL INTEGER = Python3 int
# SQL REAL = Python3 float
# SQL TEXT = Python3 str

######## tokenizer code
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
        if word.isdigit():
            tokens.append(int(word))
        else:
            tokens.append(float(word))

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

#########end of tokenize code
_ALL_DATABASES = {}

class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.dataBase = _ALL_DATABASES[filename]
        else:
            self.dataBase = Database(filename)
            _ALL_DATABASES[filename] = self.dataBase
        
    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        tokens = tokenize(statement)
        if tokens[0].upper() == "CREATE":
            self.dataBase.create(tokens)
            return []

        elif tokens[0].upper() == "INSERT":
            self.dataBase.insert(tokens)
            return []

        elif tokens[0].upper() == "SELECT":
            return self.dataBase.select(tokens)

        else:
            pass

    def close(self):  #not part of project 2.
        """
        Empty method that will be used in future projects
        """
        pass

def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)

###############################################################
class Database(object):
    """
    database object that stores relational tables/collecton of relations
    """
    def __init__(self,filename) -> None:
        self.name = filename
        self.tables = []
        pass

    def create(self,tokens):
        table = Table()
        table.name = tokens[2]
        
        p1 = 0 #index of (
        p2 = 0 #index of )
        for i in range(len(tokens)):
            if tokens[i] == "(":
                p1 = i
            if tokens[i] == ")":
                p2 = i

        headers  = ""
        for i in range(p1+1,p2):
            headers += tokens[i] + " "
        
        headers = headers.split(",")
        for item in headers:
            item = item.split()
            table.headers.append(item[0])
            if item[1]:
                table.headerTypes.append(item[1])

        self.tables.append(table)
        
    def insert(self, tokens):
        tName = tokens[2]
        currentTable = None
        for item in self.tables:
            if item.name == tName:
                currentTable = item
        
        p1 = 0 #index of (
        for i in range(len(tokens)):
            if tokens[i] == "(" and p1 == 0:
                p1 = i
        vals = tokens[p1:len(tokens)]
        values_to_add = []
        begin = 0
        i = 0
        while vals:
            if vals[i] == ";":
                break
            if vals[i] == "(":
                begin = i
                i += 1
                continue
            if vals[i] == ")":
                if vals[i+1]:
                    entry = ("".join(str(vals[i]) for i in range(begin,i+1))).strip(" ").strip("(").strip(")").split(",")
                    values_to_add.append(entry)
                    vals = vals[i+1:]
                    i = 0
                    continue
            i+=1
        for item in values_to_add:
            row = Row()
            for j in range(len(item)):
                if currentTable.headerTypes[j] == "TEXT":
                    if item[j] == 'None':
                        row.data[currentTable.headers[j]] = None
                    else:
                        row.data[currentTable.headers[j]] = item[j]
                elif currentTable.headerTypes[j] == "REAL":
                    if item[j] == 'None':
                        row.data[currentTable.headers[j]] = None
                    else:
                        row.data[currentTable.headers[j]] = float(item[j])
                elif currentTable.headerTypes[j] == "INTEGER":
                    if item[j] == 'None':
                        row.data[currentTable.headers[j]] = None
                    else:
                        row.data[currentTable.headers[j]] = int(item[j])
            currentTable.rows.append(row)
        return []

    def selectHelper(self,tokens,currentTable):
        rList = []
        for r in currentTable.rows:
                rList.append(tuple(list(r.data.values())))
            
        start  = tokens.index("BY") + 1 
        criteria = (''.join(tokens[start:len(tokens)-1])).split(",")

        for i in range(len(criteria)):
            temp = currentTable.headers.index(criteria[i]) 
            criteria[i] = temp


        rList = sorted(rList, key=itemgetter(*criteria))
        
        return rList

    def select(self, tokens):
        if tokens[1] == "*": #select all
            tName = tokens[3]
            currentTable = None
            for item in self.tables:
                if item.name == tName:
                    currentTable = item   

            return self.selectHelper(tokens,currentTable)

        else:
            end = tokens.index("FROM")
            selections = ("".join(tokens[1:end])).split(",")
            tName = tokens[end+1]
            currentTable = None
            for item in self.tables:
                if item.name == tName:
                    currentTable = item

            rList = self.selectHelper(tokens,currentTable)

            for i in range(len(selections)):
                temp = currentTable.headers.index(selections[i])
                selections[i] = temp

            sList = []
            for item in rList:
                temp = []
                for index in selections:
                    temp.append(item[index])
                sList.append(tuple(temp))
            return sList



class Table(object):
    """
    table object which has rows/table is a relation
    """
    def __init__(self) -> None:
        self.name = "" 
        self.headers = []
        self.headerTypes = []
        self.rows = []

class Row(object):
    """
    series of data/entries 
    """
    def __init__(self) -> None:
        self.data = {}

