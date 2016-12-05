//
//  YRSyncController.swift
//
//  Created by Yahia Ragae on 6/5/16.
//  Copyright © 2016 Yahia. All rights are not reserved :) .
//


import Foundation
import FMDB
import SwiftyJSON
open class YRSyncController: NSObject {
     open static func  sync(_ values : JSON , tableName:String,database : FMDatabase!){

        DispatchQueue.global(qos: DispatchQoS.QoSClass.default).async {

            objc_sync_enter(self)
            for   row      in values.array!  {
                let row = row as JSON
                let status  :  Bool =   checkIfRow(withID: Int(row["ID"].stringValue)!, existInTable: tableName,database: database )

                
                let columnsNamesBuilder : NSMutableString = ""
                let columnsValuesBuilder : NSMutableString = ""
                let updateStatmentBuilder : NSMutableString = ""
                
                var columnsNames  : String = ""
                var columnsValues : String = ""
                var updateStatment  : String = ""
                
                for value in (row.dictionaryObject?.keys)! {
                    if(value == "Status"){
                        continue
                    }
                    if(tableName == DBContontroller.TABLE_NAME_KPIS && value == "Values" ){
                        continue
                    }
                    columnsNamesBuilder.append(NSString(format: "  %@ ,",value) as String )
                    columnsValuesBuilder.append(NSString(format: " \"%@\" ,",row[value].stringValue) as String  ) ;
                    if(value != "ID"){
                        updateStatmentBuilder.append(NSString(format: "%@ = \"%@\" ,",value,row[value].stringValue) as String)
                    }
                    if(columnsNamesBuilder.length>0){
                        columnsNames  = columnsNamesBuilder.substring(to: columnsNamesBuilder.length-1)
                    }
                    if(columnsValuesBuilder.length>0){
                        columnsValues  = columnsValuesBuilder.substring(to: columnsValuesBuilder.length-1)
                    }
                    if(updateStatmentBuilder.length>0){
                        updateStatment = updateStatmentBuilder.substring(to: updateStatmentBuilder.length-1)
                    }
                    
                }
                let sqlInsert = NSString(format: "insert into %@ ( %@ )  values ( %@ )", tableName,columnsNames,columnsValues)
                let sqlUpdate = NSString(format: "update   %@ SET   %@     where ID = \"%@\" ", tableName,updateStatment,row["ID"].stringValue)
                
                
                if(status){
                    database.executeStatements(sqlUpdate as String)
                }else{
                    database.executeStatements(sqlInsert as String)
                }
            }
            objc_sync_exit(self)

        }
    }
    open static func checkIfRow(withID id:Int , existInTable tableName:String,database : FMDatabase!) -> Bool{
        let sql = "select * from \(tableName) where ID = \(id) "
        let result : FMResultSet = try!database.executeQuery(sql, values: nil)
        var status = false
        while (result.next()){
            status = true
        }
        return status
    }
    
    
}
