import pyodbc, time
def GetOption():
    print('Por favor ingrese el nombre del punto SCADA a eliminar el comando en progreso (nombre de la señal en la ventana de control)\n')
    signal_name = input()
    # if signal_name == '':
    #     print("""¿Desea eliminar todos los comandos en progreso del sistema? \n 1) Si\n 2) No""")
    #     option = input()
    #     if option == '1' or option.lower() == 'si':
    #         print("""¿Estas demente? \n 1) Si\n 2) No""")
    #         confirm = input()
    #         if confirm == '1' or confirm.lower()=='si':
    #             print("Por favor di en voz alta que si confirmas")
    #             time.sleep(10)
    #             print("Pues no mi ciela, eso solo lo puede hacer GTO, te toca copiar punto por punto ;)")
    #             # if keyboard.is_pressed('Esc'):
    #             #     sys.exit(0)
    #             # if keyboard.is_pressed('ENTER'):
    #             #     GetOption()
    #     else:
    #         print('Pues yo creo que sí')
    # #             print('Presiona Esc para salir, para continuar presione Enter')
    # #             if keyboard.is_pressed('Esc'):
    # #                 sys.exit(0)
    # #             if keyboard.is_pressed('ENTER'):
    # #                 GetOption()
    # else:
    #     return signal_name
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