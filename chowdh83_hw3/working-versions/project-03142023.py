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
    query = query[1:] #the first ' has been removed
    # end_quote_index = query.find("'")
    end_quote_index = 0

    for i in range(len(query)):
        if query[i] == "'":
            if query[i+1] == "'":
                continue
            elif query[i+1] == "," or query[i+1] == ";":
                end_quote_index = i
                break

    text = query[:end_quote_index]
    text = text.replace("''","'")

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

        if query[0] in "(),;*<>=!.":
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
        
        elif tokens[0].upper() == "DELETE":
            return self.dataBase.delete(tokens)
        
        elif tokens[0].upper() == "UPDATE":
            return self.dataBase.update(tokens)

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
        tName = tokens[2] #getting the table name 
        currentTable = None
        for item in self.tables: #getting the table to add data to
            if item.name == tName:
                currentTable = item

        columns_insert = []
        multi_insert = False
        if tokens.index("VALUES") - 2 > 1:
            columns_insert = tokens[4:tokens.index("VALUES")-1]
            columns_insert  = list(filter(lambda x: x != ",", columns_insert))
            multi_insert = True
        
        p1 = tokens.index("VALUES") + 1
        vals = tokens[p1:len(tokens)] #values to be added from tokens

        values_to_add = [] #list of list of values to add 
        begin = 0
        i = 0
        while vals: #getting all the values to add
            if vals[i] == ";":
                break
            if vals[i] == "(":
                begin = i
                i += 1
                continue
            if vals[i] == ")":
                if vals[i+1]:
                    entry = ("".join(str(vals[i]) for i in range(begin,i+1))).strip(" ").strip("(").strip(")").split(",")   #returns a list
                    values_to_add.append(entry)
                    vals = vals[i+1:]
                    i = 0
                    continue
            i+=1
        if not multi_insert:
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
        else:
            currentTable.insert_row(columns_insert,values_to_add)
        return []

    def selectHelper(self,tokens,currentTable):
        rList = []
        # changes due to delete
        if currentTable:
            for r in currentTable.rows:
                    rList.append(tuple(list(r.data.values())))
            
        start  = tokens.index("BY") + 1 
        criteria = (''.join(tokens[start:len(tokens)-1])).split(",") #getting all the criterias
        # changes due to delete
        for i in range(len(criteria)):
            if currentTable:
                temp = currentTable.headers.index(criteria[i]) #saving the indexes of the criterias
                criteria[i] = temp


        rList = sorted(rList, key=itemgetter(*criteria)) #last line to sort with respect to criterias
        
        return rList #the return is a list of tuples 

    def select(self, tokens):
        tabind = tokens.index("FROM") + 1 #the table name 
        tabName = tokens[tabind]
        tokens = list(filter(lambda x: x != tabName, tokens)) #so that qualified names can be simpler to handle
        tokens = list(filter(lambda x: x != ".", tokens))

        if tokens[1] == "*": #select all, get the table and let the helper do the rest 
            tName = tabName
            currentTable = None
            for item in self.tables:
                if item.name == tName:
                    currentTable = item   

            return self.selectHelper(tokens,currentTable)

        else:
            end = tokens.index("FROM")
            selections = ("".join(tokens[1:end])).split(",") #splitting the selected into a list

            tName = tabName #got the table name
            currentTable = None
            for item in self.tables:
                if item.name == tName:
                    currentTable = item

            rList = self.selectHelper(tokens,currentTable) #will return complete rows as list of tuples

            for i in range(len(selections)):
                temp = currentTable.headers.index(selections[i])
                selections[i] = temp #replace the selection header with its index instead

            sList = []
            for item in rList: #now we reselect as per the indexes provided in select query
                temp = []
                for index in selections:
                    temp.append(item[index])
                sList.append(tuple(temp))
            return sList

    def delete(self, tokens):
        if "WHERE" in tokens:
            for item in self.tables:
                if item.name == tokens[2]:
                    item.delete_row(tokens)
        else:
            for item in self.tables:
                if item.name == tokens[2]:
                    self.tables.pop(self.tables.index(item)) #index of table to be deleted

    def update(self, tokens):
        where_clause = False
        if "WHERE"  in tokens:
            where_clause = True

        table_name = tokens[1]
        currentTable = None
        for item in self.tables:
            if item.name == table_name:
                currentTable = item 
        currentTable.update_row(tokens,where_clause)

    def left_outer_join(self,tokens):
        return


class Table(object):
    """
    table object which has rows/table is a relation
    """
    def __init__(self) -> None:
        self.name = "" 
        self.headers = []
        self.headerTypes = []
        self.rows = []

    def delete_row(self,tokens): #will be used to delete when where clause is used. 
        column = tokens[4]
        operator = tokens[5]
        val = tokens[6]
        if operator == "!=" or operator == "IS NOT":
            self.rows = [d for d in self.rows if d.data[column] == val]

        elif operator == "=" or operator == "IS":
            self.rows = [d for d in self.rows if d.data[column] != val]

        elif operator == "<":
            self.rows = [d for d in self.rows if d.data[column] > val]
    
        elif operator == ">":
            self.rows = [d for d in self.rows if d.data[column] < val]

    def insert_row(self,columns,list_of_values): #maily for specified column insert
        for values in list_of_values:
            row = Row()
            for item in self.headers:
                row.data[item] = None
            for i in range(len(values)):
                if values[i].isdigit():
                    row.data[columns[i]] = int(values[i])
                else:
                    try:
                        row.data[columns[i]] = float(values[i])
                    except ValueError:
                        row.data[columns[i]] = str(values[i])
            self.rows.append(row)
        return
    
    def update_row(self,criteria,bool):
        if not bool:
            ind = 0
            for i in range(len(criteria)):
                if criteria[i] == "SET":
                    ind = i+1
                    break
            criteria = criteria[ind:len(criteria)-1]
            sublists = [] #will contain all the sublists of criterias rto be used
            current_sublist = []
            for element in criteria:
                if element == ',':
                    sublists.append(current_sublist)
                    current_sublist = []
                else:
                    current_sublist.append(element)
            sublists.append(current_sublist)

            for item in self.rows:
                for tag in sublists:
                    item.data[tag[0]] = tag[2]
        else:
            inds = 0
            inde = 0 #index of where
            for i in range(len(criteria)):
                if criteria[i] == "SET":
                    inds = i+1
                elif criteria[i] == "WHERE":
                    inde = i
                    break
            s_criteria = criteria[inds:inde]
            w_criteria = criteria[inde+1:len(criteria)-1] #all the criterias for where clause
            sublists = [] #will contain all the sublists of criterias rto be used
            current_sublist = []
            for element in s_criteria:
                if element == ',':
                    sublists.append(current_sublist)
                    current_sublist = []
                else:
                    current_sublist.append(element)
            sublists.append(current_sublist) 

            for item in self.rows:
                for tag in sublists:
                    if item.data[w_criteria[0]] == w_criteria[2]:
                        item.data[tag[0]] = tag[2]   
        

class Row(object):
    """
    series of data/entries 
    """
    def __init__(self) -> None:
        self.data = {}




############
# testdb = "testdb"
# con = Connection(testdb)
# con.execute("CREATE TABLE student (name TEXT, grade REAL, piazza INTEGER);")
# con.execute("INSERT INTO student VALUES ('James', 4.0, 1);") 
# con.execute("INSERT INTO student VALUES ('Yaxin', 4.0, 2);") 
# con.execute("INSERT INTO student VALUES ('Li', 3.2, 2);") 
# con.execute("UPDATE student SET grade = 3.0, piazza = 3 WHERE name ='Yaxin';")

# print(con.execute("SELECT * FROM student ORDER BY name;"))



