using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBreeze;
using DBreeze.DataTypes;
using System.Collections;
using System.IO;

namespace DbreezeSample
{
    // Helpers.DbreezePath = Path of Dbreeze engine where the files will be stored
    // Idea here is to there will be a main table that stores the Key and value (value is the path of nested table)
    //this sample program uses all the features of dbreeze , esp. the nested table concept.
    
    public static class LocalStorage
    {
              
        private static DBreezeEngine engine = new DBreezeEngine(Helpers.DbreezePath);       
        private static NestedTable table = null;
        public static DBreezeEngine newEngine = null;
        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        public static void Dispose()
        {
            if (engine != null)
                engine.Dispose();
        }

        /// <summary>
        /// Save a value with the key.
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public static void Save<Tkey, T>(Tkey key, T value)
        {
            Save<Tkey, T>("LocalData", key, value);
        }

        /// <summary>
        /// Save a value with the key.
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table name.</param>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public static void Save<Tkey, T>(String table, Tkey key, T value)
        {
            using (var tran = engine.GetTransaction())
            {               
                tran.Insert<Tkey, DbMJSON<T>>(table, key, value);               
                tran.Commit();
            }
        }

        /// <summary>
        /// Saves the specified table.
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static NestedTable Save<Tkey>(String table, Tkey key)
        {
            NestedTable nested = null;
            using (var tran = engine.GetTransaction())
            {
                nested = tran.InsertTable<Tkey>(table, key,0);
                tran.Commit();
            }
            
            return nested;
        }

        /// <summary>
        /// Saves the values from a list to a nested table
        /// </summary>
        /// <typeparam name="Tpkey">The type of the pkey.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="Tk">The type of the k.</typeparam>
        /// <typeparam name="Tk1">The type of the k1.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="pkey">The pkey.</param>
        /// <param name="value">The value.</param>
        public static void NestedSaveLoop<Tpkey, T,Tk,Tk1>(String table, Tpkey pkey, T value)
        {                       
            using (var tran = engine.GetTransaction())
            {                
                String newPath = Get<Tpkey, String>("LocalData", pkey);
                if(newEngine == null)
                    newEngine = new DBreezeEngine(Helpers.DbreezePath + "\\" + newPath);
                if (Directory.Exists(Helpers.DbreezePath + "\\" + newPath))
                {                    
                    using (var newTran = newEngine.GetTransaction())
                    {
                        newTran.SynchronizeTables(table);
                        if (value.GetType() == typeof(T))
                        {
                            IEnumerable enumerable = value as IEnumerable;
                            if (enumerable != null)
                            {
                                foreach (Tk1 val in enumerable)
                                {
                                    dynamic dyn = val as Object;
                                    var key = dyn.PageIndex;
                                    var nestedTable = newTran.InsertTable<Tpkey>(table, pkey, 0).Insert<Tk, DbMJSON<Tk1>>(key, val);
                                }
                            }
                        }

                        newTran.Commit();
                    }
                }
                                       
            }           
        }

        /// <summary>
        /// Save the object - single value
        /// </summary>
        /// <typeparam name="Tpkey">The type of the pkey.</typeparam>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="pkey">The pkey.</param>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public static void NestedSave<Tpkey, Tkey, T>(String table, Tpkey pkey, Tkey key, T value)
        {            
            using (var tran = engine.GetTransaction())
            {
                String newPath = Get<Tpkey, String>("LocalData", pkey);
                if(newEngine == null)
                    newEngine = new DBreezeEngine(Helpers.DbreezePath + "\\" + newPath);
                if (Directory.Exists(Helpers.DbreezePath + "\\" + newPath))
                {                   
                    using (var newTran = newEngine.GetTransaction())
                    {
                        newTran.SynchronizeTables(table);
                        var nestedTable = newTran.InsertTable<Tpkey>(table, pkey, 0).Insert<Tkey, DbMJSON<T>>(key, value);
                        newTran.Commit();
                    }
                }
                              
            }
        }


        /// <summary>
        /// Gets all the record associated with the key
        /// Note:Key here has nested values
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static IEnumerable<T> GetNestedValue_All<Tkey, T>(String table, Tkey key)
        {
            using (var tran = engine.GetTransaction())
            {
                String newPath = Helpers.DbreezePath + "\\" +  Get<Tkey, String>("LocalData", key);
                Boolean test = Directory.Exists(newPath);
                if(newEngine == null)
                    newEngine = new DBreezeEngine(Helpers.DbreezePath + "\\" + newPath);
                if (Directory.Exists(newPath))
                {                   
                    using (var newTran = newEngine.GetTransaction())
                    {
                        newTran.SynchronizeTables(table);
                        foreach (var row in newTran.SelectTable<Tkey>(table, key, 0).SelectForward<int, DbMJSON<T>>())
                        {
                            yield return JsonConvert.DeserializeObject<T>(row.Value.SerializedObject);
                        }
                    }
                }
                      
            }
        }

        /// <summary>
        ///  Get the specific key value.
        ///  Note: Key here is not the parent key but the nested value key
        /// </summary>
        /// <typeparam name="Tpkey">Parent Key.</typeparam>
        /// <typeparam name="Tkey">Child Key - nested key.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="pkey">The pkey.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static T GetNestedValue_WithKey<Tpkey,Tkey, T>(String table, Tpkey pkey, Tkey key)
        {
            using (var tran = engine.GetTransaction())
            {
                String newPath = Get<Tkey, String>("LocalData", key);
                if (newEngine == null)
                    newEngine = new DBreezeEngine(Helpers.DbreezePath + "\\" + newPath);
                if (Directory.Exists(Helpers.DbreezePath + "\\" + newPath))
                {                    
                    using (var newTran = newEngine.GetTransaction())
                    {
                        newTran.SynchronizeTables(table);
                        var test = newTran.SelectTable<Tpkey>(table, pkey, 0).Select<Tkey, DbMJSON<T>>(key);
                        if (test != null)
                            return JsonConvert.DeserializeObject<T>(test.Value.SerializedObject);
                        else
                            return default(T);
                    }
                }                
            }

            return default(T);
        }

        /// <summary>
        /// Gets the record count.
        /// </summary>
        /// <typeparam name="Tpkey">The type of the pkey.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="pkey">The pkey.</param>
        /// <returns></returns>
        public static Int32 GetRecordCount<Tpkey>(String table, Tpkey pkey)
        {            
            using (var tran = engine.GetTransaction())
            {
                String newPath = Get<Tpkey, String>("LocalData", pkey);
                if (newEngine == null)
                    newEngine = new DBreezeEngine(Helpers.DbreezePath + "\\" + newPath);
                if (Directory.Exists(Helpers.DbreezePath + "\\" + newPath))
                {                   
                    using (var newTran = newEngine.GetTransaction())
                    {
                        newTran.SynchronizeTables(table);
                        return Convert.ToInt32(newTran.SelectTable<Tpkey>(table, pkey, 0).Count());
                    }
                }                
            }
            return default(Int32);
        }

        /// <summary>
        /// Return true if the nested key is present
        /// </summary>
        /// <typeparam name="Tpkey">The type of the pkey.</typeparam>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="pkey">The pkey.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static Boolean CheckForKey<Tpkey, Tkey,T>(String table, Tpkey pkey, Tkey key)
        {
            Boolean returnValue = false;
            using (var tran = engine.GetTransaction())
            {
                String newPath = Get<Tpkey, String>("LocalData", pkey);
                if (newEngine == null)
                    newEngine = new DBreezeEngine(Helpers.DbreezePath + "\\" + newPath);
                if (Directory.Exists(Helpers.DbreezePath + "\\" + newPath))
                {                    
                    using (var newTran = newEngine.GetTransaction())
                    {                        
                        newTran.SynchronizeTables(table);
                        var test = newTran.SelectTable<Tpkey>(table, pkey, 0).Select<Tkey, DbMJSON<T>>(key);
                        if (test.Value != null) //check whether the key exists
                            returnValue =  true;
                    }
                }                
            }          
            return returnValue;
        }

      

        /// <summary>
        /// Gets the value specified key.
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static T Get<Tkey, T>(Tkey key)
        {
            return Get<Tkey, T>("LocalData", key);
        }
       
        /// <summary>
        /// Gets the value specified key.
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table name.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static T Get<Tkey, T>(String table, Tkey key)
        {           
            using (var tran = engine.GetTransaction())
            {
                tran.SynchronizeTables(table);
                var row = tran.Select<Tkey, DbMJSON<T>>(table, key);

                if (row.Value == null)
                    return default(T);

                return row.Value.Get;
            }
        }
       
        /// <summary>
        /// Checks for new file creation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static Int32 CheckForNewFileCreation<T>()
        {
            using (var tran = engine.GetTransaction())
            {
                tran.SynchronizeTables("SortDocument");
                return tran.SelectForward<int, DbMJSON<T>>("SortDocument").Count();
            }
        }

        /// <summary>
        /// Creates the new file.
        /// </summary>
        public static void CreateNewFile()
        {
            using (var tran = engine.GetTransaction())
            {
                tran.SynchronizeTables("SortDocument");
                RemoveTable<String>("SortDocument");        
            }
        }


        /// <summary>
        /// Gets the list values.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        public static IEnumerable<T> GetListValues<T> (String table)
        {
                using (var tran = engine.GetTransaction())
                {
                    List<T> text = new List<T>();
                   
                    foreach (var row in tran.SelectForward<int, DbMJSON<T>>(table))
                    {
                        yield return JsonConvert.DeserializeObject<T>(row.Value.SerializedObject);
                        
                    }
                   
                }           
        }

        /// <summary>
        /// Removes the specified key and value.
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static Boolean Remove<Tkey>(Tkey key)
        {           
            return Remove("LocalData", key);
        }

        /// <summary>
        /// Removes the specified key and value from the table.
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static Boolean Remove<Tkey>(String table, Tkey key)
        {
            Boolean removed = false;            
            using (var tran = engine.GetTransaction())
            {
                tran.SynchronizeTables(table);
                tran.RemoveKey<Tkey>(table, key, out removed);
                tran.Commit();                        
            }           
            return removed;
        }


        /// <summary>
        /// Removes the specified key and value from the table.
        /// </summary>
        /// <typeparam name="Tkey">The type of the key.</typeparam>
        /// <param name="table">The table.</param>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static Boolean RemoveNestedKey<Tpkey>(String table, Tpkey pkey)
        {
            Boolean removed = false;
            String newPath = Get<Tpkey, String>("LocalData", pkey);
            if (newEngine == null)
                newEngine = new DBreezeEngine(Helpers.DbreezePath + "\\" + newPath);
            if (Directory.Exists(Helpers.DbreezePath + "\\" + newPath))
            {
                using (var newTran = newEngine.GetTransaction())
                {
                    newTran.SynchronizeTables(table);
                    newTran.RemoveKey<Tpkey>(table, pkey, out removed);
                    newTran.Commit();    
                }
            }
            return removed;
        }
        /// <summary>
        /// Removes the keys from the table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        public static void RemoveTable<T>(String table)
        {
            //using (var tran = engine.GetTransaction())
            //{
            //    String newPath = Get<Tkey, String>("LocalData", key);
            //    DBreezeEngine newEngine = new DBreezeEngine(Helpers.DbreezePath + "\\" + newPath);
            //    using (var newTran = newEngine.GetTransaction())
            //    {
            //        newTran.SynchronizeTables(table);
            //        newTran.RemoveAllKeys(table, true);
            //        newTran.Commit();
            //    }
               
            //}            
        }          
    }
}
