traducciones: radio_es.qm

radio_es.qm: radio_es.ts
	lrelease radio_es.ts -compress -qm radio_es.qm

radio_es.ts: radio8.py plsparser.py addradio2.ui radio3.ui 
	pylupdate4 -verbose radio8.py plsparser.py addradio2.ui\
        radio3.ui -ts radio_es.ts
