def Sort_Tuple(rlist,criteria):
    slist = []
    for i in range(len(criteria)):
        slist = sorted(rlist, key = lambda x: x[criteria[i]])
        for j in range(len(slist)-1):
            if slist[j][criteria[i]] == slist[j+1][criteria[i]]:
                try:
                    if slist[j][criteria[i+1]] > slist[j+1][criteria[i+1]]:
                        temp = slist[j]
                        slist[j]  = slist[j+1]
                        slist[j+1] = temp
                except IndexError:
                    continue
        break
    return slist
# Driver Code
tup = [ ('James', 4.0, 1), ('Yaxin', 4.0, 2),('Li', 3.2, 2)]
tup2 = [ ('James', 3.5, 1), ('Yaxin', 2.0, 2),('Li', 2.3, 2)]
print(Sort_Tuple(tup2,[2,1]))

# for k in range(len(criteria)):
#             lst = len(rList) #3
#             for i in range(0, lst):
#                 for j in range(0, lst-i-1):
#                     if (rList[j][criteria[k]] > rList[j + 1][criteria[k]]):
#                         temp = rList[j]
#                         rList[j] = rList[j + 1]
#                         rList[j + 1] = temp
                    
#                     elif (rList[j][criteria[k]] == rList[j + 1][criteria[k]]):
#                         try:
#                             if (rList[j][criteria[k+1]] > rList[j + 1][criteria[k+1]]):
#                                 temp = rList[j]
#                                 rList[j] = rList[j + 1]
#                                 rList[j + 1] = temp
#                         except IndexError:
#                             continue