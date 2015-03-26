using HttpServer
using SQLite

global db = SQLiteDB("dattomb.db");

typealias ID Int64
typealias HTTPQuery Dict{String,String}

immutable SQLQuery
    body::String
    where::String
end

immutable SQLObjectQuery
    query::SQLQuery
    SQLObjectQuery(body::String, where::String) = new(SQLQuery(body, where))
end

abstract SQLRecord;

immutable DataSet
    id:: ID
    name:: String
end

function SQLCreateTable(::Type{DataSet})
    SQLQuery("CREATE TABLE DataSet (id BIGINT PRIMARY KEY, name VARCHAR(256))", "")
end

function SQLInsertInto(::Type{DataSet})
    SQLObjectQuery("INSERT INTO DataSet VALUES (:id, :name)", "")
end

function SQLUpdate(::Type{DataSet})
    SQLObjectQuery("UPDATE DataSet SET name = :name", "id = :id")
end

function SQLGetAll(::Type{DataSet})
    SQLQuery("SELECT * FROM DataSet", "");
end

function SQLGetRecord(set::DataSet)
    SQLQueryFilter(SQLGetAll(DataSet), string("id = ", set.id))
end

function SQLInstantiate(stmt, set::DataSet)
    values = [symbol("id") => set.id, symbol("name") => set.name];
    setFields(stmt, values);
end

function SQLRetrieveRecord(record::SQLRecord,::Type{DataSet})
    DataSet(getField(record, 1), getField(record, 2))
end

function toJSON(set:: DataSet)
    string("\"", set.name, "\"");
end

immutable DataPoint 
    id:: ID
    dataset:: ID
    tag:: String
    timestamp:: Int64
    value:: Float64
end

function SQLCreateTable(::Type{DataPoint})
    SQLQuery("CREATE TABLE DataPoint (id BIGINT PRIMARY KEY, dataset BIGINT, tag VARCHAR(256), timestamp BIGINT, value DOUBLE)", "")
end

function SQLInsertInto(::Type{DataPoint})
    SQLObjectQuery("INSERT INTO DataPoint VALUES (:id, :dataset, :tag, :timestamp, :value)", "")
end

function SQLUpdate(::Type{DataPoint})
    SQLObjectQuery("UPDATE DataPoint SET dataset = :dataset,  tag = :tag, timestamp = :timestamp, value = :value", "id = :id")
end

function SQLGetAll(::Type{DataPoint})
    SQLQuery("SELECT * FROM DataPoint", "");
end

function SQLGetRecord(point::DataPoint)
    SQLQueryFilter(SQLGetAll(DataPoint), string("id = ", point.id))
end

function SQLInstantiate(stmt, point::DataPoint)
    values = [symbol("id") => point.id, symbol("dataset") => point.dataset, symbol("tag") => point.tag, symbol("timestamp") => point.timestamp, symbol("value") => point.value];
    setFields(stmt, values);
end

function SQLRetrieveRecord(record::SQLRecord,::Type{DataPoint})
    DataPoint(getField(record, 1), getField(record, 2), getField(record, 3), getField(record, 4), getField(record, 5))
end

function toJSON(point::DataPoint)
    string("[", point.timestamp, ",", point.value, "]")
end

function SQLQueryFilter(query::SQLQuery, where::String)
    SQLQuery(query.body, string(query.where, if length(query.where) != 0 "AND" else "" end, where))
end

function SQLRetrieveQuerySet(db, recordType, query::SQLQuery)
    ret = recordType[]
    queryStr = query.body
    println(query.where)
    if (length(query.where) != 0)
        queryStr = string(queryStr, " WHERE ", query.where)
    end
    println(queryStr)
    try 
        result = getResultSet(db, queryStr)
        size = getResultSize(db, result)
        for i in [1:size]
            push!(ret, SQLRetrieveRecord(getRecord(db, result, i), recordType))
        end
    catch
    end
    return ret
end

type DataSetView
    dataSet:: DataSet
    points:: Vector{DataPoint}
end

function toJSON(view :: DataSetView)
    points = string ("[", join(map(toJSON, view.points),","), "]")
    println(points)
    string("{ \"label\" : ", toJSON(view.dataSet), ", \"data\" : ", points, "}")
end

type DataPointRequest
    point::DataPoint
    view::DataSetView
end

function getSafe(dict, key, def)
    try 
        dict[key] 
    catch 
        def 
    end
end

function fromDictionary(point::DataPointRequest, dict::Dict{String,String})
    point.point = DataPoint(uint64(getSafe(dict, "id", "0")),
                            uint64(getSafe(dict, "dataset", "0")),
                            getSafe(dict, "tag", ""),
                            int64(getSafe(dict, "timestamp", "0")),
                            float64(getSafe(dict, "value", "0")));
end

function toDictionary(point::DataPointRequest)
    ret = Dict{String,String}();
    ret["id"] = string(point.point.id);
    ret["dataset"] = string(point.point.dataset);
    ret["tag"] = point.point.tag;
    ret["timestamp"] = string(point.point.timestamp);
    ret["value"] = string(point.point.value);
    ret["data_view"] = toJSON(point.view)
    return ret;
end

function parseQueryVariable(var::String, query::HTTPQuery)
    pair = split(var, "=");
    if length(pair) >= 2 
        query[pair[1]] = pair[2];
    end
end

function parseQuery(var::String)
    args = split(var, "?");
    query::HTTPQuery = Dict{String,String}();
    if length(args) >= 2 
        map(x->parseQueryVariable(x, query), split(args[2], "&"));
    end
    return query;
end

type ModelAttribute
    value
end

typealias Model Dict{String, Union(String,ModelAttribute)}

function addModelAtribute(model::Model, name::String, attribute_value)
    model[name]=ModelAttribute(attribute_value)
end

immutable WebPage
    addModelAttributes::Function
    action::Function
end

function queryToModel(page::WebPage, query::HTTPQuery)
    cache::Dict{String, Dict{String, String}} = Dict{String, Dict{String, String}}();
    model::Model = Dict{String,Union(String,ModelAttribute)}();
    page.addModelAttributes(model);
    for (k,v) in query
        println (string(k, "->", v))
        path=split(k,".")
        if length(path) == 1
            model[path[1]] = v
        elseif length(path) > 1
         if !haskey(cache, path[1])
             cache[path[1]] = Dict{String,String}();
         end
         cache[path[1]][path[2]] = v
        end
    end
    for (k, v) in cache
        if !haskey(model, k)
            continue;
        end
        attr = model[k]
        if typeof(attr) != ModelAttribute
            continue;
        end
        fromDictionary(attr.value, v)
    end
    return model
end

function replaceTemplateVariables(x, model::Model)
    arg::String = match(r"%%([^%]*)%%",x).captures[1];
    if haskey(model, arg)
        return model[arg]
    end
    parts = split(arg, ".");
    if !haskey(model, parts[1])
        return ""
    end
    attr = model[parts[1]];
    if typeof(attr) == ModelAttribute
        attrDict = toDictionary(attr.value)
        for (k,v) in attrDict
            model[string(parts[1],".", k)] = v
        end
       delete!(model, parts[1])
    end
    if haskey(model, arg)
        return model[arg]
    end
    ""
end

function modelToHTMLPage(pageName::String, model::Model)
    file = open(string("templates/", pageName,".html"), "r")
    str = readall(file);
    replace(str, r"%%[^%]*%%", x -> replaceTemplateVariables(x, model))
end

function exampleAddModelAttributes(model::Model)
    addModelAtribute(model, "point", DataPoint(0,0,"",zero(Float64)))
end

function exampleAction(model::Model)
    println(string("tag: ", model["point"].value.tag))
end

function AddPointModel(model::Model)
    point = DataPoint(0,0,"",0,zero(Float64))
    view = DataSetView(DataSet(0, ""), [])
    addModelAtribute(model, "point", DataPointRequest(point, view))
end

function AppPointPage(model::Model)
    println(db)
    println(string("tag: ", model["point"].value.point.tag))
    point = model["point"].value.point;
    points = SQLRetrieveQuerySet(db, DataPoint, SQLGetRecord(point));
    println(points)
    if !haskey(model, "action")
        if length(points) != 0
            point = points[1]
        end
    elseif length(points) == 0
        point = DataPoint(round(time() * 10^6), point.dataset, point.tag, point.timestamp, point.value)
        println(point)
        newObject(db, point)
    else
        updateObject(db, point)
    end

    model["point"].value.point = point
    model["point"].value.view = DataSetView(DataSet(1, "test dataset"), 
                                            SQLRetrieveQuerySet(db, DataPoint, SQLGetAll(DataPoint)));
#[DataPoint(1,1, "", 1, 10), DataPoint(1,1, "", 2, 20)]);
    "add_point"
end

function readfile(filename::String, def)
    try
        file = open(filename, "r")
        return readall(file);
    catch
        return def
    end
end

http = HttpHandler() do req::Request, res::Response 
    if ismatch(r"^/resources/css/[^/]*$", req.resource)
        return Response(readfile(req.resource[2:end], 404), (String=>String)["Content-Type" => "text/css"])
    elseif ismatch(r"^/resources/js/[^/]*$", req.resource)
        return Response(readfile(req.resource[2:end], 404), (String=>String)["Content-Type" => "text/javascript"])
    elseif ismatch(r"^/add_point/",req.resource) 
        page = WebPage(AddPointModel, AppPointPage);
    else
        return Response(404)
    end

    model = queryToModel(page, parseQuery(req.resource));
    pageName = page.action(model);
    
    Response(modelToHTMLPage(pageName, model))
end

immutable SQLiteRecord <: SQLRecord
    set::ResultSet
    index::Int32
end

function setField(stmt::SQLiteStmt, index, value)
    bind(stmt, index, value)
end

function setFields(stmt::SQLiteStmt, values)
    bind(stmt, values)
end

function getField(record::SQLiteRecord, index)
    record.set[index][record.index]
end

function getRecord(::SQLiteDB, resultSet, index)
    SQLiteRecord(resultSet, index)
end

function getResultSize(::SQLiteDB, resultSet)
    size(resultSet, 1)
end

function getResultSet(db::SQLiteDB, queryStr::String)
    query(db, queryStr)
end

function newObject(db::SQLiteDB, object)
    println(db)
    println(SQLInsertInto(typeof(object)).query.body)
    stmt = SQLiteStmt(db, SQLInsertInto(typeof(object)).query.body);
    SQLInstantiate(stmt, object);
    execute(stmt);
    close(stmt)
end

function updateObject(db::SQLiteDB, object)
    updatequery = SQLUpdate(typeof(object))
    stmt = SQLiteStmt(db, string (updatequery.query.body, " WHERE ", updatequery.query.where));
    SQLInstantiate(stmt, object);
    execute(stmt);
    close(stmt)
end

function createTable(db::SQLiteDB, table)
    stmt = SQLiteStmt(db, SQLCreateTable(table).body);
    execute(stmt);
    close(stmt);
end

try
    stmt = SQLiteStmt(db, SQLCreateTable(DataPoint).body);
    execute(stmt);
    close(stmt);
    stmt = SQLiteStmt(db, SQLInsertInto(DataPoint).query.body);
    SQLInstantiate(stmt, DataPoint(1, 1, "test", 1000, 10));
    execute(stmt);
    SQLInstantiate(stmt, DataPoint(2, 1, "test tag 2", 1001, 20));
    execute(stmt);
    close(stmt);
catch
end
updatequery = SQLUpdate(DataPoint);
stmt = SQLiteStmt(db, string (updatequery.query.body, " WHERE ", updatequery.query.where));
SQLInstantiate(stmt, DataPoint(1,1, "test updated", 1000, 11));
execute(stmt)
close(stmt)
points = SQLRetrieveQuerySet(db, DataPoint, SQLQuery("select ID from DataPoint", ""));
for point in points
    println("id: ", point.id, " tag: ", point.tag)
end

# HttpServer supports setting handlers for particular events
http.events["error"]  = ( client, err ) -> println( err )
http.events["listen"] = ( port )        -> println("Listening on $port...")

server = Server( http ) #create a server from your HttpHandler
run( server, 8000 ) #never returns
close(db);
