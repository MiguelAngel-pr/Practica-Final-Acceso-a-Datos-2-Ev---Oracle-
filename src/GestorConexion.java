import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author migue
 */
public class GestorConexion 
{
    Connection conn1 = null; //Variable que almacena la conexión de la BBDD
    String bbdd = "PELICULAS";//String que almacena la base de datos que usaremos, si se cambia se usará otra
    public GestorConexion()
    {
        abrir_conexion();
    }
    public int abrir_conexion()//Método usado para conectarse a la BBDD
    {
        try
        {
            String url = "jdbc:oracle:thin:@localhost:1521/PELICULAS";
            String user = "SYSTEM";
            String password = "1234";
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn1 = DriverManager.getConnection(url, user, password);
            if(conn1 != null)
            {
                System.out.println("Te has conectado correctamente");
                return 0;
            }
            else
            {
                System.out.println("Ha habido un error al conectarte");
                return -1;
            }
        }
        catch(SQLException | ClassNotFoundException ex)
        {
            System.out.println(ex);
            return -1;
        }
    }
    
    public String[] obtenerTitulos(String tabla, String tipo)//Método con el que se obtiene una lista con todos los campos o las tablas
    {
        Statement sta;
        String nombre_tabla[] = null; 
        String query = "";
        try 
        {
            sta = conn1.createStatement();
            if(tipo.equals("tablas")) //Dependiendo de si es una tabla o un campo se realizará una consulta u otra.
            {
                nombre_tabla = new String [obtenerNTablas()]; 
                query = "SELECT OBJECT_NAME AS nombre FROM USER_OBJECTS WHERE OBJECT_TYPE = 'TABLE' and SHARING = 'NONE'";
            }
            else if(tipo.equals("campos"))
            {
                nombre_tabla = new String [obtenerNCampos(tabla)]; 
                query = "SELECT COLUMN_NAME AS nombre FROM cols WHERE TABLE_NAME= '" + tabla + "'";
            }
            ResultSet rs = sta.executeQuery(query);
            int i=0;
            while (rs.next())
            {
                nombre_tabla[i]=rs.getString("nombre");
                i++;
            }
            rs.close();
            sta.close();
        } 
        catch (SQLException ex) 
        {
            System.out.println("Error2");
        }
        return nombre_tabla;
    }
    
    public int obtenerNCampos(String tabla)//Método con el que se obtiene el nº de campos de una tabla
    {
        Statement sta;
        int nColumnas = 0;
        try 
        {
            sta = conn1.createStatement();
            String query = "Select * from " + tabla + "";
            ResultSet rs = sta.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            nColumnas = rsmd.getColumnCount();
            rs.close();
            sta.close();
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return nColumnas;
    }
    
    public int obtenerNTablas()//Método con el que se obtiene el nº de tablas de una base de datos
    {
        Statement sta;
        int nTablas = 0;
        try 
        {
            sta = conn1.createStatement();
            String query = "SELECT COUNT(OBJECT_NAME) AS count FROM USER_OBJECTS WHERE OBJECT_TYPE = 'TABLE' and SHARING = 'NONE'";
            ResultSet rs = sta.executeQuery(query);
            if(rs.next())
            {
                nTablas = rs.getInt("count");
            }
            rs.close();
            sta.close();
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return nTablas;
    }
    
    public int insertaDatos(String tabla, String campos[], String[] valores, int ncampos)//Método con el que se insertan datos en una tabla
    {
        Statement sta;
        try 
        {
            sta = conn1.createStatement();
            String tcampos = "";
            String tvalores = "";
            String[] añadidoREF = {"",""};
            String pk = "";
            for(int i = 0; i<ncampos; i++)
            {
                tcampos = tcampos + "" + campos[i] + "";
                if(!valores[i].isEmpty())
                {
                    if(valores[i].length()==10 && valores[i].substring(4,5).equals("-") && valores[i].substring(7, 8).equals("-"))
                    {
                        tvalores = tvalores + "TO_DATE('" + valores[i] + "','YYYY-MM-DD')";
                    }
                    else
                    {
                        String tablaFK = obtenerFK(tabla, campos[i]);
                        if(!tablaFK.equals(""))
                        {
                            if(añadidoREF[0].equals(""))
                            {
                                pk = obtenerPK(tablaFK);
                                tvalores = tvalores + "REF(x)";
                                añadidoREF[0] = " FROM " + tablaFK + " x ";
                                añadidoREF[1] = "WHERE x." + pk + " = '" + valores[i] + "'";
                            }
                            else
                            {
                                tvalores = tvalores + "REF(y)";
                                if(!obtenerPK(tablaFK).equals(pk))
                                {
                                    pk = obtenerPK(tablaFK);
                                    añadidoREF[0] = añadidoREF[0] + ", " + tablaFK + " y ";
                                    añadidoREF[1] = añadidoREF[1] + " AND y." + pk + " = '" + valores[i] + "'";
                                }
                                else
                                {
                                    añadidoREF[1] = añadidoREF[1] + " AND x." + pk + " = '" + valores[i] + "'";
                                }
                            }
                        }
                        else
                        {
                            tvalores = tvalores + "'" + valores[i] + "'";
                        }
                    }
                }
                else
                {
                    tvalores = tvalores + "NULL";
                }
                
                if((i+1)!=ncampos)
                {
                     tcampos = tcampos + ",";
                     tvalores = tvalores + ",";
                }     
            }
            String query = "INSERT INTO " + tabla + " (" + tcampos + ") VALUES (" + tvalores + ")";
            if(!añadidoREF[0].equals(""))
            {
                query = "INSERT INTO " + tabla + " (SELECT " + tvalores + "" + añadidoREF[0] + " " + añadidoREF[1] + ")";
            }
            System.out.println(query);
            sta.executeUpdate(query);
            
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            System.out.println(ex);
            return -1;
        }
    }
    
    public int modificaCampo(String tabla, String campo, String valorAntiguo, String valorNuevo, String id)//Método con el que se modifica un campo ya existente
    {
        Statement sta;
        try 
        {
            sta = conn1.createStatement();
            String pk = obtenerPK(tabla);
            String id_where= "";
            String tipo = "";
            if(!id.equals(""))
            {
                id_where = " AND " + pk + " = '" + id + "'";
            }
            if(valorAntiguo.length()>=10 && valorAntiguo.substring(4,5).equals("-") && valorAntiguo.substring(7, 8).equals("-"))
            {
                valorAntiguo = "TO_DATE('" + valorAntiguo.substring(0, 10)+ "','YYYY-MM-DD')";
                valorNuevo = "TO_DATE('" + valorNuevo + "','YYYY-MM-DD')";
                tipo = "fecha";
            }
            if(valorNuevo.equals(""))
            {
                sta.executeUpdate("UPDATE " + tabla + " SET " + campo + " = NULL WHERE " + campo + " = '" + valorAntiguo + "' " + id_where + "");
            }
            else
            {
                if(valorAntiguo.equals("-"))
                {
                    String query = "UPDATE " + tabla + " SET " + campo + " = '" + valorNuevo + "' WHERE " + campo + " IS NULL " + id_where + "";
                    if(valorNuevo.length()>=10 && valorNuevo.substring(4,5).equals("-") && valorNuevo.substring(7, 8).equals("-"))
                    {
                        valorNuevo = "TO_DATE('" + valorNuevo + "','YYYY-MM-DD')";
                        query = "UPDATE " + tabla + " SET " + campo + " = " + valorNuevo + " WHERE " + campo + " IS NULL " + id_where + "";
                    }
                    System.out.println(query);
                    sta.executeUpdate(query);
                }
                else
                {
                    String query = "UPDATE " + tabla + " SET " + campo + " = '" + valorNuevo + "' WHERE " + campo + " = '" + valorAntiguo + "' " + id_where + "";
                    if(tipo.equals("fecha"))
                    {
                        query = "UPDATE " + tabla + " SET " + campo + " = " + valorNuevo + " WHERE " + campo + " = " + valorAntiguo + " " + id_where + "";
                    }
                    System.out.println(query);
                    sta.executeUpdate(query);
                }
            } 
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            System.out.println("Error");
            return -1;
        }
    }
    
    public String obtenerPK(String tabla)//Método con el que se obtiene el nombre de la clave primaria de una tabla
    {
        Statement sta;
        String pk = "";
        try 
        {
            sta = conn1.createStatement();
            String query = "SELECT column_name AS nombrepk FROM all_cons_columns WHERE constraint_name = (SELECT constraint_name from all_constraints WHERE TABLE_NAME= '" + tabla + "' AND CONSTRAINT_TYPE = 'P')";
            ResultSet rs = sta.executeQuery(query);
            if(rs.next())
            {
                pk = rs.getObject("nombrepk")+"";
            }
            rs.close();
            sta.close();
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return pk;
    }
    
    public String obtenerFK(String tabla, String campo)//Método usado para identificar la tabla a la que hace referencia la foreign key
    {
        System.out.println("ENTRADA FK: " + campo);
        Statement sta;
        String tablas[] = obtenerTitulos("","tablas");
        String fktabla = "";
        try 
        {
            sta = conn1.createStatement();
            String query = "select DATA_TYPE as tabla from cols WHERE TABLE_NAME= '" + tabla + "' AND COLUMN_NAME= '" + campo + "'";
            ResultSet rs = sta.executeQuery(query);
            if(rs.next())
            {
                fktabla = rs.getObject("tabla")+"";
                for(int i=0; i<tablas.length; i++)
                {
                    if(fktabla.contains(tablas[i]))
                    {
                        fktabla = tablas[i];
                        return fktabla;
                    }
                }
                fktabla = "";
            }
            rs.close();
            sta.close();
            return fktabla;
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return fktabla+"";
    }
    
    public int borrarFila(String tabla, String campo, String valor, String id) //Método que permite borrar una fila elegida de una tabla 
    {
        Statement sta;
        try 
        {
            String pk = obtenerPK(tabla);
            String id_where= "";
            if(!id.equals(""))
            {
                id_where = " AND " + pk + " = '" + id + "'";
            }
            sta = conn1.createStatement();
            String query = "DELETE FROM " + tabla + " WHERE " + campo + " ='" + valor + "'" + id_where + "";
            sta.executeUpdate(query);
            sta.close();
            return 0;
        } 
        catch (SQLException ex) 
        {
            return -1;
        }
    }
    
    public ResultSet realizaConsulta(String tablaElegida, String campo, String operacion, String valor)//Método con el que se realizan las consultas
    {
        Statement sta;
        ResultSet rs = null;
        try 
        {
            sta = conn1.createStatement();
            String query = "";
            if(campo.equals("") || campo.equals("*"))
            {
                query = "Select * from " + tablaElegida + "";  
            }
            else
            { 
                if(operacion.equals(">") || operacion.equals("<"))
                {
                    if(!valor.equals(""))
                    {
                        query = "SELECT * FROM " + tablaElegida + " WHERE " + campo + " " + operacion + valor + "";
                    }
                }
                else
                {
                    String tablaFK = obtenerFK(tablaElegida, campo);
                    if(!tablaFK.equals(""))
                    {
                        String[] camposFK = obtenerTitulos(tablaElegida, "campos");
                        String pk = obtenerPK(tablaFK);
                        String seleccion = "";
                        for(int i=0; i<camposFK.length; i++)
                        {
                            seleccion = seleccion + "x." + camposFK[i];
                            if(i+1 != camposFK.length)
                            {
                                seleccion = seleccion + ",";
                            }
                        }
                        query = "SELECT " + seleccion + " FROM " + tablaElegida + " x, " + tablaFK + " y WHERE y." + pk + " = '" + valor + "' AND x." + campo + " = REF(y)";
                        System.out.println(query);
                    }
                    else
                    {
                        query = "SELECT * FROM " + tablaElegida + " WHERE " + campo + " LIKE '%" + valor + "%'";
                    }
                    System.out.println(query);
                }
                if(valor.equals("null") || valor.equals("NULL"))
                {
                    query = "Select * from " + tablaElegida + " WHERE " + campo + " IS NULL";  
                }
            }
            rs = sta.executeQuery(query);
            return rs;    
        } 
        catch (SQLException ex) 
        {
            System.out.println(ex);
            rs = null;
        }
        return rs;
    }
    
    public String[] realizaConsultaCampo(String tablaElegida, String campo)//Método con el que se obtiene los valores de un campo especifico
    {
        Statement sta;
        String [] resultado = null;
        String pk = obtenerPK(tablaElegida);
        String query = "";
        try 
        {
            sta = conn1.createStatement();
            if(!pk.equals(""))
            {
                query = "SELECT " + campo + " as valor FROM " + tablaElegida + " ORDER BY " + pk + ""; 
                System.out.println(query);
            }
            else
            {
                query = "SELECT " + campo + " as valor FROM " + tablaElegida + ""; 
            } 
            ResultSet rs = sta.executeQuery(query);
            sta = null;
            
            //Mido la longitud de la consulta para formar el array
            sta = conn1.createStatement();
            String query2 = "SELECT COUNT(*) as longitud FROM " + tablaElegida + "";
            ResultSet rs_count = sta.executeQuery(query2);
            rs_count.next();
            int nColumnas = rs_count.getInt("longitud");
            resultado = new String[nColumnas];
            
            //añado los valores que he sacado de la consulta al array
            int i = 0;
            while (rs.next())
            {
                resultado[i]=rs.getObject("valor")+"";
                i++;
            }
            return resultado;    
        } 
        catch (SQLException ex) 
        {
            Logger.getLogger(GestorConexion.class.getName()).log(Level.SEVERE, null, ex);
        }
        return resultado;
    }
    
    public String[] realizaConsultaREF(String tablaElegida, String campo, String campoBusqueda, String fktabla)//Metodo usado para obtener una lista del dato al que eligas de la tabla a la q hace referencia un REF
    {
        String[] fkdatos = new String[100];
        Statement sta;
        try 
        {
            sta = conn1.createStatement();
            String query = "select DEREF(" + campo + ")." + campoBusqueda + " as dato FROM " + tablaElegida + ""; 
            //System.out.println(query);
            ResultSet rs = sta.executeQuery(query);
            int i = 0;
            while(rs.next())
            {
                fkdatos[i]=rs.getObject("dato")+"";
                i++;
            }
            return fkdatos;    
        } 
        catch (SQLException ex) 
        {
            fkdatos = null;
        }
        return fkdatos;
    }
    
    public int cerrar_conexion()//Método con el que se cierra la conexión de la BBDD
    {
        try
        {
            System.out.println("Te has desconectado correctamente");
            conn1.close();
            return 0;
        }
        catch(SQLException ex)
        {
            System.out.println("ERROR");
            return -1;
        }
    }
}
