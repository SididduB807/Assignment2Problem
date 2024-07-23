#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/
#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

from pymongo import MongoClient

def connectDataBase():
    """Connect to the MongoDB database and return the database object."""
    client = MongoClient('mongodb://localhost:27017/')
    db = client['assignment2']  
    return db

def createCategory(col, catId, catName):
    """Create a category in the collection."""
    category = {
        '_id': catId,
        'name': catName
    }
    col.insert_one(category)

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    """Create a document in the collection and update the inverted index."""
    document = {
        '_id': docId,
        'text': docText,
        'title': docTitle,
        'date': docDate,
        'category': docCat
    }
    col.insert_one(document)
    updateIndex(col)

def deleteDocument(col, docId):
    """Delete a document from the collection and update the inverted index."""
    col.delete_one({'_id': docId})
    updateIndex(col)

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    """Update a document in the collection and update the inverted index."""
    updated_document = {
        'text': docText,
        'title': docTitle,
        'date': docDate,
        'category': docCat
    }
    col.update_one({'_id': docId}, {'$set': updated_document})
    updateIndex(col)

def getIndex(col):
    """Generate the inverted index from the documents in the collection."""
    index = {}
    for doc in col.find():
        docId = doc['_id']
        docTitle = doc['title']
        docText = doc['text'].lower()
        terms = docText.split()
        for term in terms:
            if term in index:
                index[term] = f"{index[term]}, {docTitle}:{docId}"
            else:
                index[term] = f"{docTitle}:{docId}"
    return index

def updateIndex(col):
    """Update the inverted index in the database."""
    index = getIndex(col)
    index_col = col.database['index']
    index_col.delete_many({})
    for term, doc_list in index.items():
        index_col.insert_one({'term': term, 'documents': doc_list})
