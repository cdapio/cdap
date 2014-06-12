.. -*- mode: rst -*-

h1h1h1
------

h2h2h2h2
--------

::

    #!-*- coding: utf8 -*-
    """Cliente serie para la balanza nc3m"""

    import struct
    import serial
    import decimal

    def decimal_from_nc3m(nc3m_num):
        """Toma un numero en el formato NC3M y lo convierte a decimal"""
        nc3m_num = nc3m_num.replace(',', '.')
        return decimal.Decimal(nc3m_num)

    def main():
        #definimos el string de formato
        fcn = 'c8sc7s2c'
        #creamos una conexión serie
        ser = serial.Serial('vserial2')
        totalizador = 0
        #Adquirimos los datos
        while True:

            a = ser.readline() #Leemos una linea del buffer
            if len(a) == 19:
                stx, neto, status, tara, cr, lf = struct.unpack(fcn, a)
                if status == ' ':  #Chequeamos que la balanza esté en equilibrio
                    neto = decimal_from_nc3m(neto)
                    totalizador += neto
                    print "Peso Neto: %s Peso Acumulado: %s" % ( neto, totalizador)

    if __name__ == "__main__":
        print "Cliente serie para balanza NC3M"
        main()
