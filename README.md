# Working with big data databases in Delphi – Cassandra, Couchbase and MongoDB (Part 3 of 3)

This final part of our trilogy on big data databases introduces a Delphi driver for MongoDB. This driver is independent from any other data access frameworks in Delphi and provides direct and efficient access to data on a MongoDB server.

![](MongoDB.png)

The previous posts in this series focus on [Cassandra](https://blog.grijjy.com/2017/01/05/working-with-big-data-databases-in-delphi-cassandra-couchbase-and-mongodb-part-1-of-3/) and [Couchbase](https://blog.grijjy.com/2017/01/11/working-with-big-data-databases-in-delphi-cassandra-couchbase-and-mongodb-part-2-of-3/).

For more information about us, our support and services visit the [Grijjy homepage](http://www.grijjy.com) or the [Grijjy developers blog](http://blog.grijjy.com).

The source code and unit tests are available in our [DelphiMongoDB](https://github.com/grijjy/DelphiMongoDB) repository on GitHub. It has a dependency on our [GrijjyFoundation](https://github.com/grijjy/GrijjyFoundation) repository, so make sure the pull the latest version of that repository as well. For ease of access, it is recommended to add the path to the GrijjyFoundation source code to your Delphi library path.

## Introduction to MongoDB

[MongoDB](https://www.mongodb.com/) is an open-source NoSQL document database designed for scalability. It uses BSON (a binary version of JSON) for both data storage and API calls (such as CRUD operations). It is one of the most popular - if not *the* most popular - NoSQL databases available.

If you don't yet have access to a MongoDB server, then it is easy to set one up. If you want to experiment with MongoDB locally, then you can install the server on your own computer. Go to the [MongoDB Download Center](https://www.mongodb.com/download-center) and select the "Community Server" tab. Download the version for your Windows configuration. You probably want to download the "Windows Server 2008 R2 64-bit and later" version, which runs on Windows 7 and later as well.

> Note that the recent versions of MongoDB do not support 32-bit Windows versions anymore.

After installation, you want to create a directory to store the MongoDB databases (for example: `C:\MongoDB\Data`). You can then start the server (or daemon) from the command prompt:

`"C:\Program Files\MongoDB\Server\3.4\bin\mongod" --dbpath C:\MongoDB\Data\`

replacing the paths to the daemon and database as needed.

> Remember to enclose any path in double quotes if it contains spaces.

For more detailed installation instructions, please refer to the [Install MongoDB Community Edition](https://docs.mongodb.com/manual/administration/install-community/) page of the manual. Here you will also find information on how to install MongoDB on non-Windows operating systems.

## A Delphi driver for MongoDB

In the remainder of this post, we present our own driver for working with MongoDB. This is certainly not the only solution available. If you use Delphi Enterprise (or purchased the FireDAC Client/Server pack for Delphi professional), then you can use FireDAC to work with MongoDB. Our driver is more low-level in the sense that you use it to talk directly to MongoDB, without an intermediate data access layer. You will lose some of the advantages that an abstraction layer provides, but you will gain advantages in terms of efficiency by not forcing an SQL-like model onto a NoSQL database. This is important in creating scalable backends. Also, the driver works with the regular Delphi Professional edition.

The driver uses [our JSON/BSON library](our efficient JSON/BSON library) that we presented in a previous post. For the transport layer, it uses our [Scalable HTTP/S and TCP client sockets](https://blog.grijjy.com/2017/01/09/scalable-https-and-tcp-client-sockets-for-the-cloud/) (see also [Part 2](https://blog.grijjy.com/2017/03/15/scalable-http-sockets-for-the-cloud-part-2/) and [Part 3](https://blog.grijjy.com/2017/04/17/scalable-linux-sockets-for-the-cloud-part-3/)) for Windows and Linux. This means that the MongoDB drivers does not work on iOS and Android. But since you should restrict database access to the backend anyway, this should not be an issue.

The API is modeled somewhat after .the official [C# and .Net MongoDB driver](https://docs.mongodb.com/ecosystem/drivers/csharp/), but the implementation is different. The driver is by no means complete compared to the .Net driver, but it supports the most commonly used operations. In fact, the backends of our own apps use this driver for all database operations. Of course, we welcome pull requests or other kinds of contributions...

## Getting Started

The easiest way to get started with the Delphi driver is to pull the [DelphiMongoDB](https://github.com/grijjy/DelphiMongoDB) and [GrijjyFoundation](https://github.com/grijjy/GrijjyFoundation) repositories and run the unit tests in the MongoDBTests project. By default, the unit tests assume that the MongoDB daemon is running on your local computer. If you want to connect to another server instead, then you should update the constants `TEST_SERVER_HOST` and/or `TEST_SERVER_PORT` in the unit `Tests.Grijjy.MongoDB.Settings` accordingly.

> Our underlying transport layer uses OpenSSL for secure connections. This means that you need to deploy the DLLs `libeay32.dll` and `ssleay32.dll` with your application on Windows. Confusingly, the 64-bit versions of these DLLs have the same name. Therefore, in the DelphiMongoDB repository, you will find these DLLs in different directories (`Bin32` and `Bin64`). The unit test build to these directories. On Linux you don't have to deploy additional files, but make sure the OpenSSL libraries are installed for things to operate correctly.

The unit tests in the unit `Tests.Grijjy.MongoDB.Samples` are ideal for experimenting with the driver. These are based on corresponding unit tests for the C# driver. The remainder of this post looks at some of the features.

## Connecting to a Server

The main entry point to the API is the `IgoMongoClient` interface. You use it to create a connection to the server and access a specific database:

```delphi
var
  Client: IgoMongoClient;
  Database: IgoMongoDatabase;
  Collection: IgoMongoCollection;
begin
  Client := TgoMongoClient.Create('localhost');
  Database := Client.GetDatabase('test');
  Collection := Database.GetCollection('restaurants');
end;
```

This creates a connection to a MongoDB server running on the local machine, using the default port (27017). It retrieves a collection named "restaurants" from the database named "test".

> The 3 APIs in the example above are very light-weight. They don't actually connect to the server or open the database yet. That is only done as soon as you start reading, writing or querying a collection in the database. Also if the database "test" or the collection "restaurants" doesn't exist yet, then they will be created automatically once you start writing to them.

## Inserting Documents

The unit tests contain an embedded resource with a sample collection in JSON format. You can find this collection in the "dataset.zip" file in the Resources subdirectory. This file contains over 25,000 restaurants and is imported using the following code snippet:

```delphi
var
  Line: String;
  Doc: TgoBsonDocument;
  DataSet: TList<TgoBsonDocument>;
  Collection: IgoMongoCollection;
begin
  ..Initialization of Stream, Dataset and Collection not shown here..
  Reader := TStreamReader.Create(Stream, TEncoding.UTF8);
  while (not Reader.EndOfStream) do
  begin
    Line := Reader.ReadLine;
    Doc := TgoBsonDocument.Parse(Line);
    Dataset.Add(Doc);
  end;
  Collection.InsertMany(Dataset);
end;
```

The text file contains one JSON document per line, which is parsed into a `TgoBsonDocument` (which is presented in the article on our [JSON and BSON library](https://blog.grijjy.com/2017/01/30/efficient-and-easy-to-use-json-and-bson-library/)) and added to a list of documents. Then, the entire list is inserted into the restaurants collection using the bulk operation `IgoMongoCollection.InsertMany`. This API works with arrays of documents or any classes derived from `TEnumerable<TgoBsonDocument>` (such as a `TList<TgoBsonDocument>`).

You can also manually create and insert a single document, as shown in the `TTestInsertPrimer` unit test:

```delphi
procedure TTestInsertPrimer.InsertADocument;
var
  Doc: TgoBsonDocument;
  Collection: IgoMongoCollection;
begin
  Doc := TgoBsonDocument.Create()
    .Add('address', TgoBsonDocument.Create()
                      .Add('street', '2 Avenue')
                      .Add('zipcode', '10075')
                      .Add('building', '1480')
                      .Add('coord', TgoBsonArray.Create([73.9557413, 40.7720266])))
    .Add('borough', 'Manhattan')
    .Add('cuisine', 'Italian')
    .Add('grades', TgoBsonArray.Create([
      TgoBsonDocument.Create()
        .Add('date', EncodeDateTime(2014, 10, 1, 0, 0, 0, 0))
        .Add('grade', 'A')
        .Add('score', 11),

      TgoBsonDocument.Create()
        .Add('date', EncodeDateTime(2014, 1, 6, 0, 0, 0, 0))
        .Add('grade', 'B')
        .Add('score', 17)]))
    .Add('name', 'Vella')
    .Add('restaurant_id', '941704620');

  Collection := Database.GetCollection('restaurants');
  Collection.InsertOne(Doc);
end;
```

The example uses the fluent interface of `TgoBsonDocument` to create a complex document using a single statement, which is then inserted using the `IgoMongoCollection.InsertOne` API. For reference, the document looks like this in JSON syntax (or more precisely, MongoDB shell syntax):

```javascript
{ 
    "address" : {
        "street" : "2 Avenue", 
        "zipcode" : "10075", 
        "building" : "1480", 
        "coord" : [
            73.9557413, 
            40.7720266
        ]
    }, 
    "borough" : "Manhattan", 
    "cuisine" : "Italian", 
    "grades" : [
        {
            "date" : ISODate("2014-10-01T00:00:00.000+0000"), 
            "grade" : "A", 
            "score" : NumberInt(11)
        }, 
        {
            "date" : ISODate("2014-01-06T00:00:00.000+0000"), 
            "grade" : "B", 
            "score" : NumberInt(17)
        }
    ], 
    "name" : "Vella", 
    "restaurant_id" : "941704620"
}
```

This clearly shows that MongoDB is a document database. Instead of having multiple tables linked with primary and foreign keys, related data is embedded in a document as a sub-document or array. Take a moment to look at the structure of this JSON document, as we will refer to it a couple of times in this post.

## Querying Data

To query data, `IgoMongoCollection` provides the `Find` and `FindOne` APIs. The difference between these two is that `FindOne` stops the search as soon as the first document that matches a query has been found.

To find all documents in a collection, simply call `Find` without parameters:

```delphi
procedure TTestQueryPrimer.QueryAll;
var
  Collection: IgoMongoCollection;
  Count: Integer;
  Doc: TgoBsonDocument;
begin
  Collection := Database.GetCollection('restaurants');
  Count := 0;

  for Doc in Collection.Find() do
    Inc(Count);

  Assert.AreEqual(25359, Count);
end;
```

`Find` returns in instance of `IgoMongoCursor`. A cursor is enumerable, meaning that you can use a `for..in` loop to enumerate over all documents in the result set. A cursor is memory and bandwidth efficient and does *not* return all documents at once into a single big array. Instead, MongoDB will offer the data in small batches (for example, 100 documents at a time). This means that while enumerating the cursor, the driver may make additional calls to the server to ask for additional batches of documents.

If you *do* want all documents into a single array, then you can use `IgoMongoCursor.ToArray`, but beware that this can result in a big array and numerous calls to the server.

### Filtering Data

Usually however, you only care about data that matches a certain filter (or query). Filters are a bit like `WHERE` clauses in SQL, but written in JSON syntax. You can write these filters yourself (using the syntax described in the [manual](https://docs.mongodb.com/manual/reference/operator/query/)), but is usually easier and less error-prone to build a filter using the `TgoMongoFilter` type:

```delphi
procedure TTestQueryPrimer.QueryTopLevelField;
begin
  ...
  for Doc in Collection.Find(TgoMongoFilter.Eq('borough', 'Manhattan')) do
    Inc(Count);
    
  Assert.AreEqual(10259, Count);
end;
```

This searches for restaurants where the "borough" field equals "Manhattan" (by using the `TgoMongoFilter.Eq` method).

> Note that the second parameter of the `Eq` method is of type `TgoBsonValue`. This type has a bunch of implicit conversion operators, which means that you can also pass strings, integers, floating-point values, Booleans and some other types.

`TgoMongoFilter` is not an interface or class type. Instead it is a record type, that is backed by an interface for the actual implementation (to provide automatic memory management). The reason for this model is that this allows us to use operator overloading to combine multiple filters together:

```delphi
procedure TTestQueryPrimer.LogicalAnd;
begin
  ...
  for Doc in Collection.Find(
    TgoMongoFilter.Eq('cuisine', 'Italian') and
    TgoMongoFilter.Eq('address.zipcode', '10075')
  ) do
    Inc(Count);

  Assert.AreEqual(15, Count);
end;

```

This example uses the overloaded logical "`and`" operator to search for restaurants in the 10075 zip-code area that serve Italian cuisine. This example also shows how to query an embedded document: Each restaurant document has a field called "address", which contains an embedded document. One of the fields in this embedded document is the "zipcode" (refer the the example JSON document earlier).

Besides searching for exact matches using `TgoMongoFilter.Eq`, you can also use a myriad of other operators, such as `Lt` to search for values less than a certain value:

```delphi
procedure TTestQueryPrimer.LessThan;
begin
  ...
  for Doc in Collection.Find(TgoMongoFilter.Lt('grades.score', 10)) do
    Inc(Count);

  Assert.AreEqual(19065, Count);
end;

```

In the example database, "grades" is an array of embedded documents, where each document has a field called "score". This example searches all restaurants where at least on of the scores in the array is less than 10.

As I mentioned, MongoDB filters are just JSON documents. The filter in the example above is equivalent to this JSON document:

```javascript
{ grades.score {$lt: 10} }
```

If you prefer to write your filters like this, then you can certainly do that. `TgoMongoFilter` has in implicit operator that converts a string to a filter, so you can use this code as well:

```delphi
  for Doc in Collection.Find('{ grades.score {$lt: 10} }') do
    Inc(Count);
```

There are lots of other operators you can use to filter data. See the documentation of the `TgoMongoFilter` type for all of them.

### Projections

By default, MongoDB returns all fields in matching documents. If you only need a subset of these fields, then you add an optional `TgoMongoProjection` argument to the `Find` or `FindOne` method. This is similar to the a `SELECT` clause in a SQL statement.

For example, to only return the "name" and "borough" fields of each restaurant that matches a certain filter:

```delphi
for Doc in Collection.Find(
  TgoMongoFilter.Eq('borough', 'Manhattan'),
  TgoMongoProjection.Include('name') +
  TgoMongoProjection.Include('borough')) 
do
  ...
```

Again, `TgoMongoProjection` is implemented as a Delphi record, and its "+" operator is overloaded so you can combine multiple projections. But in this case, it is probably easier to use the overloaded version of `Include` where you can pass multiple fields in an array:

```delphi
for Doc in Collection.Find(
  TgoMongoFilter.Eq('borough', 'Manhattan'),
  TgoMongoProjection.Include(['name', 'borough'])) 
do
  ...
```

Also, you can write the projections in JSON syntax instead of you prefer.

In addition to the `Include` method, there are some other methods as well that you can use to customize the returned documents.

### Sorting

Finally, you can sort the returned data by passing an optional `TgoMongoSort` modifier to `Find` or `FindOne`:

```delphi
for Doc in Collection.Find(
  TgoMongoFilter.Empty,
  TgoMongoSort.Ascending('borough') +
  TgoMongoSort.Ascending('address.zipcode')) 
do
```

This example sorts the documents by borough first and by zip-code next. Here, `TgoMongoFilter.Empty` is used as an alternative way to return all documents in the collection. Beware of this though, as the sort operation may fail if the returned dataset is too large (more than 32 MB).

## Deleting Data

Filters are also used to delete documents that match a query. For example, to delete all restaurants in Manhattan:

```
procedure TTestRemovePrimer.RemoveMatchingDocuments;
var
  Collection: IgoMongoCollection;
  Count: Integer;
begin
  Collection := Database.GetCollection('restaurants');
  Count := Collection.DeleteMany(TgoMongoFilter.Eq('borough', 'Manhattan'));
  Assert.AreEqual(10259, Count);
end;
```

## Updating Data

To update a document, you also need a filter to specify which document (or documents) you want to update. In addition, you need a `TgoMongoUpdate` definition, that specifies how the document should be updated.

The following example searches for a restaurant with a certain ID and updates it street address (that is, the "street" field of the embedded "address" document). It also updates the "lastModified" field to the current date and time. If the "lastModified" field does not exist, then it will be added.

```delphi
Collection.UpdateOne(
  TgoMongoFilter.Eq('restaurant_id', '41156888'),
  TgoMongoUpdate.Init
    .&Set('address.street', 'East 31st Street')
    .CurrentDate('lastModified));
```

`TgoMongoUpdate` works a little bit different then types mentioned so far. You always start by calling the static `Init` function. After that, you can use a fluent interface and call methods such as `Set` and `CurrentDate` to build the update definition. There are lots of other possible update operations. See the documentation of `TgoMongoUpdate` for all of them.

As with all other CRUD record types, you can also write update definitions in JSON format if you prefer.

In addition to `UpdateOne`, there is also an `UpdateMany` method that you can use to update multiple documents at once.

## Handling errors

Many APIs return an integer or Boolean value with the result of the operation. APIs that work on a single document (such as `DeleteOne`) return `True` if the operation succeeded. APIs that work on multiple documents (such as `Find` and `InsertMany`) return the number of document affected.

If an error occurs, then an exception derived from `EgoMongoDBError` is raised. There are usually two types of errors. 

First, you have connection errors of type `EgoMongoDBConnectionError`. These are raised when a connection to the MongoDB server fails, or when an operation times out.

Most other errors relate to problems writing to the database. These are of type `EgoMongoDBWriteError` and contain an additional error code.

## License

DelphiMongoDB is licensed under the Simplified BSD License. See License.txt for details.