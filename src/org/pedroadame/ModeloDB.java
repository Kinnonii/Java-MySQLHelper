package org.pedroadame;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ResultSetMetaData;
import com.mysql.jdbc.Statement;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gestor de consultas a bases de datos en Java.
 *
 * @author Nesto
 * @author Pedro Adame
 * @version 2.0
 */
public class ModeloDB {

    public static ModeloDB instance = null;

    /**
     * Nombre de la base de datos donde se opera.
     */
    private String nombrebd = null;

    /**
     * Usuario de la base de datos.
     */
    private String usuario = null;

    /**
     * Contraseña del usuario
     */
    private String passwd = null;

    /**
     * Host que almacena la base de datos
     */
    private String host = null;

    /**
     * Se sabe si el singleton ha sido configurado. El objeto solo funciona si el campo está a true
     * Solo se puede poner a true mediante el metodo configure.
     */
    private boolean configured = false;

    private Connection conexion = null;

    private String url = null;

    /**
     * Constructor que impide la instanciación desde fuera de la clase.
     */
    private ModeloDB() {
    }

    /**
     * Devuelve un singleton de ModeloDB. Si es la primera llamada, se debe configurar llamando a configure()
     *
     * @return Singleton de ModeloDB
     */
    public static ModeloDB getInstance() {
        if (instance == null) {
            instance = new ModeloDB();
        }
        return instance;
    }

    /**
     * Configura el Singleton en base a los parámetros utilizados.
     * Este método es el acceso principal al singleton.
     * Para futuros accesos sin reconfigurar, se debe utilizar el método getInstance()
     *
     * @param bd     Nombre de la base de datos.
     * @param user   Usuario de la base de datos.
     * @param passwd Contraseña del usuario.
     * @param host   Host de la base de datos.
     * @return Singleton de ModeloDB ya configurado.
     */
    public static ModeloDB configure(String bd, String user, String passwd, String host) {
        ModeloDB md = getInstance();
        if (bd == null || bd.isEmpty() || user == null || user.isEmpty() || passwd == null || passwd.isEmpty() || host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Parámetros de configuración no válidos. Ninguno puede ser nulo ni estar vacío.");
        }
        md.host = host;
        md.passwd = passwd;
        md.usuario = user;
        md.nombrebd = bd;
        md.url = "jdbc:mysql://" + md.host + "/" + md.nombrebd;
        md.configured = true;
        return md;
    }

    private boolean establecerConexion() {
        boolean flag = false;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conexion = (Connection) DriverManager.getConnection(url, usuario, passwd);
            flag = true;
        } catch (ClassNotFoundException | SQLException ex) {
            ex.printStackTrace();
        }
        return flag;
    }

    private void cerrarConexion() {
        try {
            if (conexion != null && !conexion.isClosed())
                conexion.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * Se ejecuta una consulta de tipo UPDATE, INSERT o DELETE.
     * Se devuelve true si correcto, false si error.
     * @param query Se pasa la consulta SQL  por parametro.
     */

    public boolean consultaSimple(String query) {
        if (!this.configured) {
            throw new IllegalStateException("ModeloDB no configurado!");
        }
        boolean flag = false;
        if (this.establecerConexion()) {
            // El try-with-resource automaticamente cierra el objeto entre
            // parentesis al finalizar el try/catch.
            // Nos ahorramos el finally{consulta.close()}
            try (Statement consulta = (Statement) conexion.createStatement()) {
                if (consulta != null) {
                    consulta.executeUpdate(query);
                    flag = true;
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                cerrarConexion();
            }
        }
        return flag;
    }

    /**
     * Ejecuta una consulta tipo SELECT se devuelve un List<Map>
     * Se devuelve NULL si hay fallo.
     * @param query Se pasa la consulta SQL  por parametro
     * @return Devuelve un ArrayList Map correspondiente
     * a cada columna de la columna.
     */
    public List<Map> consultaConResultado(String query) {
        if (!this.configured) {
            throw new IllegalStateException("ModeloDB no configurado!");
        }
        List<Map> fila = null;
        if (this.establecerConexion()) {
            // El try-with-resource automaticamente cierra el objeto entre
            // parentesis al finalizar el try/catch.
            // Nos ahorramos el finally{consulta.close()}
            try (Statement consulta = (Statement) conexion.createStatement()) {
                if (consulta != null) {
                    ResultSet rs = consulta.executeQuery(query);
                    ResultSetMetaData meta = (ResultSetMetaData) rs.getMetaData();
                    int columnas = meta.getColumnCount();
                    fila = new ArrayList<>();
                    while (rs.next()) {
                        Map<String, String> columna = new HashMap<String, String>();
                        for (int i = 1; i <= columnas; i++) {
                            String clave = meta.getColumnName(i);
                            String valor = rs.getString(i);
                            columna.put(clave, valor);
                        }
                        fila.add(columna);
                    }

                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            } finally {
                cerrarConexion();
            }
        }
        return fila;
    }

}
