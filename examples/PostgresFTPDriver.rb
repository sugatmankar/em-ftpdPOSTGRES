require 'tempfile'
require 'pg'
require 'em-ftpd'
require 'eventmachine'
require 'dbi'
class PostgresFTPDriver
  
attr_accessor :current_dir ,:current_dirid,:dirlist,:file

  def change_dir(path, &block)
   
    directory_name = path.match(/([^\/.]*)$/)
   
    new_directory_name = "/"+directory_name[0]
  case path
    
  when path then
    
  begin
       connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
       
       connection.prepare('dir_statement','select FOLDER_NAME from FOLDERS where FOLDER_NAME=$1 ')
    
       result = connection.exec_prepared('dir_statement',[new_directory_name])
               
         if result.count == 1
           
           currentdir(path,result.getvalue(0,1))

           yield true           
         
         else   
         
           yield false         
                      
         end   
    
    rescue Exception => e
      
      puts e.message
      
    ensure
      closedb(connection)
    
    end
    
    when ".." then
      
      begin
       connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
       
       connection.prepare('dir_statement','select PARENT_FOLDER from FOLDERS where PARENT_FOLDER=$1 and FOLDER_NAME=$2')
    
       result = connection.exec_prepared('dir_statement',[current_dirid||'1',path])
               
         if result.count == 1
           
           currentdir(path,result.getvalue(0,1))
                      
           yield true           
         
         else   
         
           yield false         
                      
         end   
    
    rescue Exception => e
      
      puts e.message
      
    ensure
      closedb(connection)
    
    end
    
    else 
      yield true
      
    end
  end
  
  def make_dir(path, &block)
   
   newdirname = path.match(/([^\/.]*)$/)
   
   new_directory_name = "/"+newdirname[0]
   
    begin
      
       connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
      
       connection.prepare('insert_into_folder_statement','insert into FOLDERS (FOLDER_NAME,PARENT_FOLDER) values ($1,$2)')
              
       result = connection.exec_prepared('insert_into_folder_statement',[new_directory_name,current_dirid||'1'])    
              yield true         
                         
         
    rescue Exception => e
      
      puts e.message
      
    ensure
      closedb(connection)
    
    end
    
  end
  
  def authenticate(user, pass, &block)
      
   yield user=="test" && pass=="1234"
       
  end
  
  def put_file(path, data, &block)
    
    newfilename = path.match(/([^\/.]*)$/)
   
    nfilename = "/"+newfilename[0]

    begin
       connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
    
       connection.prepare('put_file_statment','insert into FILES (FILE_NAME,FILE_DATA,PARENT_FOLDER) values ($1,$2,$3)')
    
       result = connection.exec_prepared('put_file_statment',[nfilename,data,current_dirid||"1"])    
          
           yield true                   
          
    rescue Exception => e
      
      puts e.message
      
    ensure
      closedb(connection)
    
    end
    
  end

  def put_file_streamed(path, data , &block)
   
    newfilename = path.match(/([^\/]*)$/)
    
    nfilename = "/"+newfilename[0]
    
    begin
       connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
    
       connection.prepare('streamed_statement','insert into FILES (FILE_NAME,PARENT_FOLDER) values ($1,$2)')
    
       result = connection.exec_prepared('streamed_statement',[nfilename,current_dirid||"1"])    
                     
       connection.prepare('update_strm_stmt','update FILES set FILE_DATA = FILE_DATA || $1 where FILE_NAME = $2 and PARENT_FOLDER = $3')
       
       data.on_stream { |chunk|
         result1 = connection.exec_prepared('update_strm_stmt',[chunk,nfilename,current_dirid||"1"])
                      }  
       yield true                   
          
     rescue Exception => e
      
     puts e.message
      
     ensure
          
    end
       
  end
  
  def delete_file(path, &block)
   
   yield false
    
  end

  def delete_dir(path, &block)
    
   yield false
    
  end
 
  def dir_contents(path, &block)
     
     path1 = path.match(/([^\/.]*)$/)
   
     path = "/"+path1[0]
  case path
    
  when "/" then    
     
    begin
          connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
                     
          connection.prepare('folder_dir_stmt','select FOLDER_NAME from FOLDERS where PARENT_FOLDER=$1')
             
          connection.prepare('file_dir_stmt', 'select FILE_NAME,FILE_DATA from FILES where PARENT_FOLDER=$1')    
                
          result2 = connection.exec_prepared('folder_dir_stmt',[current_dirid||'1'])
          
          result3 = connection.exec_prepared('file_dir_stmt',[current_dirid||'1'])          
                         
              
               @dirlist = Array.new
                  k =0                         
            
               result2.each do |row1|                                 
                  val = result2.getvalue(k,0)                                 
               
                  val = val.tr('^A-Za-z0-9.', '')
               
                  @dirlist[k] = dir_item(val)                  
                                        
                  k = k+1                  
               end 
                                         
               
                result3.each_with_index do |row2,m|
                                 
                  val = result3.getvalue(m,0)                                    
                      
                    val = val.tr('^A-Za-z0-9.', '')
               
                 
                  @dirlist[k] = file_item(val,'20')
                     
                  m = m+1                      
                  k = k+1
                  
               end           
           
            yield [ *dirlist ]               
           
          
    rescue Exception => e
      
      puts e.message
      
    ensure
      
      closedb(connection)
    
    end       
     
     when path then    
    
        path =  "/"+path.tr('^A-Za-z0-9.', '')
   
     begin
          connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
                     
          connection.prepare('stmt4','select FOLDER_NAME from FOLDERS where PARENT_FOLDER=$1')
             
          connection.prepare('stmt5', 'select FILE_NAME,FILE_DATA from FILES where PARENT_FOLDER=$1')    
                
          result2 = connection.exec_prepared('stmt4',[current_dirid||'1'])
          
          result3 = connection.exec_prepared('stmt5',[current_dirid||'1'])          
                         
              
               @dirlist = Array.new
                  k =0                         
            
               result2.each do |row1|                                 
                  val = result2.getvalue(k,0)                                 
                      
                   val = val.tr('^A-Za-z0-9.', '')
                  @dirlist[k] = dir_item(val)                  
                                        
                  k = k+1                  
               end 
                             
                result3.each_with_index do |row2,m|
                                 
                  val = result3.getvalue(m,0)    
                   val = val.tr('^A-Za-z0-9.', '')                                
                      
                  @dirlist[k] = file_item(val,'60')
                     
                  m = m+1                      
                  k = k+1
                  
               end           
           
            yield [ *dirlist ]               
           
          
    rescue Exception => e
      
      puts e.message
      
    ensure
      
      closedb(connection)
    
    end       
   else
     
      yield []
      
      end        
     
  end
  
  def get_file(path, &block)
     
     
     filename = path.match(/([^\/]*)$/)
    
     nfilename = "/"+filename[0]
     
     
     begin
   
       connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
    
       connection.prepare('stmt1','select FILE_NAME,FILE_DATA from FILES where FILE_NAME=$1')
    
          
       result = connection.exec_prepared('stmt1',[nfilename])       
       
          fdata = result.getvalue(0,1)
                               
          file = Tempfile.new('tempfile')
               
          file.write fdata
        
          name = file.path
   
          @newfilenam = name.match(/([^\/.]*)$/)
     
          @newfilename = @newfilenam[0]
                                         
          yield file.path
                              
    rescue Exception => e
      
      puts e.message
      
    ensure
      
      closedb(connection)
         
    end
    
  end
 
  def bytes(path, &block)
      filename = path.match(/([^\/.]*)$/)
    
    nfilename = "/"+filename[0]
    
    begin
       connection = DBI.connect("DBI:Pg:test:localhost","postgres","postgres")
       connection.prepare('stmt1','select FILE_DATA from FILES where FILE_NAME=$1')              
    
       res = connection.exec_prepared('stmt1',[nfilename])
    
       data = res.getvalue(0,0)
       
       yield data.size
       
       
         
    rescue Exception => e
      
      puts e.message
      
    ensure
      closedb(connection)
    
    end
  end
  
    
private

  def dir_item(name)
        
      EM::FTPD::DirectoryItem.new(:name => name, :directory => true, :size => 0)
               
  end

  def file_item(name,bytes)
    EM::FTPD::DirectoryItem.new(:name => name, :directory => false, :size => bytes)
  
  end
  

  def closedb(connection)
        connection.disconnect if connection
   end

  def currentdir(path="/",id="1")
  
  @current_dir = path
  @current_dirid = id
  
  
  end  
end
  


# configure the server
driver PostgresFTPDriver
#driver_args 1, 2, 3
#user "ftp"
#group "ftp"
#daemonise false
#name "fakeftp"
#pid_file "/var/run/fakeftp.pid"
