import pyodbc, time
def GetOption():
    print('Por favor ingrese el nombre del punto SCADA a eliminar el comando en progreso (nombre de la señal en la ventana de control)\n')
    signal_name = input()
    return signal_name
    

def main():
    print('Conectando')
    signal_name = GetOption()
    conn = pyodbc.connect('DSN=pcsRTDB')
    cur = conn.cursor() 
    #print('Por favor ingrese el nombre del punto SCADA a eliminar el comando en progreso (nombre de la señal en la ventana de control)\n')
    print('Se va a eliminar un flexTag en el punto',signal_name)
    query = "delete from flexTag;".format(signal_name)
    #query = "DELETE from FlexTag;".format(signal_name)
    print('Ejecutando el Query: ',query)
    cur.execute(query)
    conn.commit()
    print('Eliminado el comando en progreso del punto ',signal_name)
    time.sleep(5)
if __name__ == '__main__':
    main()