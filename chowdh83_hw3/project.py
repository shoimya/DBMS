"""
Name: Shoimya Chowdhury
Netid: chowdh83
PID:159958342
How long did this project take you? too long a solid 48


Sources:
no resources other than searching how to use filter and index fucntion etc
"""
import string
from operator import itemgetter
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
            elif query[i+1] == "," or query[i+1] == ";"or query[i+1] == ")"or query[i+1] == " ":
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
            table.headers.append(table.name+"."+item[0])  #adding the table headers 
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
            for i in range(len(columns_insert)):
                if "." not in columns_insert[i]:
                    columns_insert[i] = currentTable.name + "." + columns_insert[i]
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
                            row.data[currentTable.headers[j]] = None #as headers are qualified the dict key will always be qualified *******
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
        criteria = (''.join(tokens[start:len(tokens)-1])).split(",")
        for i in range(len(criteria)):
            if "." not in criteria[i]:
                criteria[i] = currentTable.name +"."+criteria[i]
        for i in range(len(criteria)):
            criteria[i] = currentTable.headers.index(criteria[i])

        return rList,criteria #the return is a list of tuples 

    def select(self, tokens):
        # All select statements will have "FROM" and "ORDER BY" clauses
        tabind = tokens.index("FROM") + 1  
        tabName = tokens[tabind] #the table name
        distinct = False
        currentTable = None #will be the first table in join
        for item in self.tables:
            if item.name == tabName:
                currentTable = item #get the table

        if not currentTable:
            return
        if "DISTINCT" in tokens:
            distinct = True
            tokens.remove("DISTINCT")
        if "JOIN" in tokens:
            table2ind = tokens.index("JOIN") + 1
            table2name = tokens[table2ind]
            secondTable = None
            for item in self.tables:
                if item.name == table2name:
                    secondTable = item #get the table
            primaryTable = currentTable.getrows()
            secondaryTable = secondTable.getrows()
            returnlist = []
            fromind = tokens.index("FROM")
            selected = (''.join(tokens[1:fromind])).split(",")
            
            for i in range(len(selected)):
                if currentTable.name in selected[i]:
                    selected[i] = currentTable.headers.index(selected[i])
                else:
                    selected[i] = secondTable.headers.index(selected[i]) #possible to make separate index lists then do the selection
                    

            onindex = tokens.index("ON")+1
            orderbyindex = tokens.index("ORDER")
            joincriteria = (''.join(tokens[onindex:orderbyindex])).split("=")
            for i in range(len(joincriteria)):
                if currentTable.name in joincriteria[i]:
                    joincriteria[i] = currentTable.headers.index(joincriteria[i])
                else:
                    joincriteria[i] = secondTable.headers.index(joincriteria[i])

            ordercriteria = (''.join(tokens[orderbyindex+2:len(tokens)-1])).split(",")
            for i in range(len(ordercriteria)):
                if currentTable.name in ordercriteria[i]:
                    ordercriteria[i] = currentTable.headers.index(ordercriteria[i])

            primaryTable = sorted(primaryTable, key=itemgetter(*ordercriteria)) #now the primary is sorted 


            for i in range(len(primaryTable)):
                found = False
                for j in range(len(secondaryTable)):
                    if primaryTable[i][joincriteria[0]] == secondaryTable[j][joincriteria[1]]:
                        returnlist.append((primaryTable[i][selected[0]],secondaryTable[j][selected[1]]))
                        found = True
                if not found:
                    returnlist.append((primaryTable[i][selected[0]],None))

            return returnlist
        else:
            fromindex = tokens.index("FROM")
            selected = (''.join(tokens[1:fromindex])).split(",") #all the selected columns
            selected_index = []
            for i in range(len(selected)):
                if "." not in selected[i]:
                    selected[i] = currentTable.name + "." + selected[i]
            
            for i in range(len(selected)):
                if "*" not in selected[i]:
                    selected_index.append(currentTable.headers.index(selected[i]))
                else:
                    for j in range(len(currentTable.headers)):
                        selected_index.append(j)

            result,sorting_criteria = self.selectHelper(tokens,currentTable) #all the rows non sorted
            return_list = []  # the list to be returned 
            filtered = []
            if "WHERE" in tokens:
                whereindex = tokens.index("WHERE") + 1
                orderindex = tokens.index("ORDER")
                wherecriteria = tokens[whereindex:orderindex]
                if "." not in wherecriteria:
                    wherecriteria[0] = currentTable.name + "." + wherecriteria[0]
                    wherecriteria[0] = currentTable.headers.index(wherecriteria[0])
                else:
                    op1 = wherecriteria.pop(wherecriteria.index(tabName))
                    op2 = wherecriteria.pop(wherecriteria.index("."))
                    op3 = wherecriteria.pop(0) #studnet . grade
                    qualifiedname = op1+op2+op3
                    wherecriteria.insert(0,qualifiedname)
                    wherecriteria[0] = currentTable.headers.index(wherecriteria[0])

                length = len(wherecriteria)
                for item in result:
                    if length == 3 and wherecriteria[1] == "=":
                        if item[wherecriteria[0]] == wherecriteria[2]:
                            filtered.append(item)
                    elif length == 4 and "!" in wherecriteria:
                        if item[wherecriteria[0]] != wherecriteria[3]:
                            filtered.append(item)
                    elif length == 3 and wherecriteria[1] == ">":
                        if wherecriteria[2] != None and item[wherecriteria[0]] is not None: #sql does not permit none comparisons 
                            if item[wherecriteria[0]] > wherecriteria[2]:
                                filtered.append(item)
                    elif length == 3 and wherecriteria[1] == "<":
                        if wherecriteria[2] != None and item[wherecriteria[0]] is not None:
                            if item[wherecriteria[0]] < wherecriteria[2]:
                                filtered.append(item)
                    elif length == 3 and None in wherecriteria:
                        if item[wherecriteria[0]] == None:
                            filtered.append(item)
                    elif length == 4 and None in wherecriteria:
                        if item[wherecriteria[0]] != None:
                            filtered.append(item)
            else:
                filtered=False

            if filtered == False: #there was no where clause
                result = sorted(result, key=itemgetter(*sorting_criteria)) #sorting before selection
                for item in result:
                    temp = []
                    for index in selected_index:
                        temp.append(item[index])
                    return_list.append(tuple(temp))
            else:
                if None in tokens and "NOT" not in tokens:
                    for item in filtered: #as where is present will make it from filtered list
                        temp = []
                        for index in selected_index:
                            temp.append(item[index])
                        return_list.append(tuple(temp))
                else:   
                    filtered = sorted(filtered, key=itemgetter(*sorting_criteria)) #sorting efore selection
                    for item in filtered: #as where is present will make it from filtered list
                        temp = []
                        for index in selected_index:
                            temp.append(item[index])
                        return_list.append(tuple(temp))
            
            if distinct: #only for single column
                return_list = list(set(return_list))
                return_list = sorted(return_list)
                return return_list
            else:
                return return_list

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
        column = self.name+"."+tokens[4]
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

    def insert_row(self,columns,list_of_values): #mainly for specified column insert
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
    
    def getrows(self):
        rList = []
        for r in self.rows:
            rList.append(tuple(list(r.data.values())))

        return rList
    
    def update_row(self,tokens,where): 
        setindex = tokens.index("SET") + 1
        whereindex = 0
        criteria = None #set criteria
        if where:
            whereindex = tokens.index("WHERE")
            criteria = tokens[setindex:whereindex]
        else:
            criteria = tokens[setindex:len(tokens)-1]
        
        sublists = [] #will have list of list of criteria inner list will have only 3 vals
        current_sublist = []
        for element in criteria:
            if element == ',':
                current_sublist[0] = self.name + "." + current_sublist[0]
                sublists.append(current_sublist)
                current_sublist = []
            else:
                current_sublist.append(element)
        current_sublist[0] = self.name + "." + current_sublist[0]
        sublists.append(current_sublist)

        if not where:
            for item in self.rows:
                for tag in sublists:
                    item.data[tag[0]] = tag[2]
        else:
            w_criteria = tokens[whereindex+1:len(tokens)-1] #where clause will always have one argument
            w_criteria[0] = self.name + "." + w_criteria[0]
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

