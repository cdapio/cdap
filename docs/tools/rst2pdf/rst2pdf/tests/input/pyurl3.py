# -*- coding: utf-8 -*-
'''Un acortador de URLs pero que permite:

* Editar adonde apunta el atajo más tarde
* Eliminar atajos
* Definir tests para saber si el atajo es válido

'''

import os
import string
import datetime

from twill.commands import go, code, find, notfind, title
def minitwill(url, script):
    '''Dada una URL y un script en una versión limitada
    de twill, ejecuta ese script.
    Apenas una línea falla, devuelve False.

    Si todas tienen éxito, devuelve True.

    Ejemplos:

    >>> minitwill('http://google.com','code 200')
    ==> at http://www.google.com.ar/
    True
    
    >>> minitwill('http://google.com','title bing')
    ==> at http://www.google.com.ar/
    title is 'Google'.
    False
    
    '''
    go (url)
    for line in script.splitlines():
        cmd,arg = line.split(' ',1)
        try:
            if cmd in ['code','find','notfind','title']:
                r = globals()[cmd](arg)
        except:
            return False
    return True


# Usamos storm para almacenar los datos
from storm.locals import *

# FIXME: tengo que hacer más consistentes los nombres
# de los métodos.

class Atajo(object):
    '''Representa una relación slug <=> URL
    
    Miembros:

    id     = Único, creciente, entero (primary key)
    url    = la URL original
    test   = un test de validez de la URL
    user   = el dueño del atajo
    activo = Si este atajo está activo o no.
             Nunca hay que borrarlos, sino el ID puede volver
             atrás y se "recicla" una URL. ¡Malo, malo, malo!
    status = Resultado del último test (bien/mal)
    ultimo = Fecha/hora del último test
    '''

    # Hacer que los datos se guarden via Storm
    __storm_table__ = "atajo"
    id     = Int(primary=True)
    url    = Unicode()
    test   = Unicode()
    user   = Unicode()
    activo = Bool()
    status = Bool()
    ultimo = DateTime()

    def __init__(self, url, user, test=''):
        '''Exigimos la URL y el usuario, test es opcional,
        _id es automático.'''

        # Hace falta crear esto?
        r = self.store.find(Atajo, user = user, url = url) 
        self.url = url
        self.user = user
        self.activo = True
        # Test por default, verifica que la página exista.
        self.test = u'code 200'
        if r.count():
            # FIXME: esto creo que es una race condition
            # Existe la misma URL para el mismo usuario,
            # reciclamos el id y el test, pero activa.
            viejo = r.one()
            Atajo.store.remove(viejo)
            self.id = viejo.id
            self.test = viejo.test
        self.store.add(self)
        # Autosave/flush/commit a la base de datos
        self.save()

    def save(self):
        '''Método de conveniencia'''
        Atajo.store.flush()
        Atajo.store.commit()

    @classmethod
    def init_db(cls):
        # Creamos una base SQLite
        if not os.path.exists('pyurl.sqlite'):
            cls.database = create_database("sqlite:///pyurl.sqlite")
            cls.store = Store (cls.database)
            try:
                # Creamos la tabla
                cls.store.execute ('''
                CREATE TABLE atajo (
                    id INTEGER PRIMARY KEY,
                    url VARCHAR,
                    test VARCHAR,
                    user VARCHAR,
                    activo TINYINT,
                    status TINYINT,
                    ultimo TIMESTAMP
                ) ''' )
                cls.store.flush()
                cls.store.commit()
            except:
                pass
        else:
            cls.database = create_database("sqlite:///pyurl.sqlite")
            cls.store = Store (cls.database)

    # Caracteres válidos en un atajo de URL
    validos = string.letters + string.digits

    def slug(self):
        '''Devuelve el slug correspondiente al
        ID de este atajo

        Básicamente un slug es un número en base 62, representado usando
        a-zA-Z0-9 como "dígitos", y dado vuelta (más significativo
        a la derecha.

        Ejemplo:

        100000 => '4aA'
        100001 => '5aA'

        '''
        s = ''
        n = self.id
        while n:
            s += self.validos[n%62]
            n = n // 62
        return s

    @classmethod
    # FIXME: no estoy feliz con esta API
    def get(cls, slug = None, user = None, url = None):
        ''' Dado un slug, devuelve el atajo correspondiente.
        Dado un usuario:
            Si url es None, devuelve la lista de sus atajos
            Si url no es None , devuelve *ese* atajo
        '''
        
        if slug is not None:
            i = 0
            for p,l in enumerate(slug):
                i += 62 ** p * cls.validos.index(l)
            return cls.store.find(cls, id = i, activo = True).one()
            
        if user is not None:
            if url is None:
                return cls.store.find(cls, user = user, activo = True)
            else:
                return cls.store.find(cls, user = user,
                    url = url, activo = True).one()

    def delete(self):
        '''Eliminar este objeto de la base de datos'''
        self.activo=False
        self.save()

    def run_test(self):
        '''Correr el test con minitwill y almacenar
        el resultado'''
        self.status = minitwill(self.url, self.test)
        self.ultimo = datetime.datetime.now()
        self.save()

# Usamos bottle para hacer el sitio
import bottle

# Middlewares
from beaker.middleware import SessionMiddleware
from authkit.authenticate import middleware
from paste.auth.auth_tkt import AuthTKTMiddleware

@bottle.route('/logout')
def logout():
    bottle.request.environ['paste.auth_tkt.logout_user']()
    if 'REMOTE_USER' in bottle.request.environ:
        del bottle.request.environ['REMOTE_USER']
    bottle.redirect('/')

@bottle.route('/')
@bottle.view('usuario.tpl')
def alta():
    """Crea un nuevo slug"""

    # Requerimos que el usuario esté autenticado.
    if not 'REMOTE_USER' in bottle.request.environ:
        bottle.abort(401, "Sorry, access denied.")
    usuario = bottle.request.environ['REMOTE_USER'].decode('utf8')

    # Data va a contener todo lo que el template
    # requiere para hacer la página
    data ={}

    # Esto probablemente debería obtenerse de una
    # configuración
    data['baseurl'] = 'http://localhost:8080/'

    # Si tenemos un parámetro URL, estamos en esta
    # funcion porque el usuario envió una URL a acortar.
    
    if 'url' in bottle.request.GET:
        # La acortamos
        url = bottle.request.GET['url'].decode('utf8')
        a = Atajo(url=url, user=usuario)    
        data['short'] = a.slug()
        data['url'] = url

        # La probamos
        a.run_test()
        
        # Mensaje para el usuario de que el acortamiento
        # tuvo éxito.
        data['mensaje'] = u'''La URL <a href="%(url)s">%(url)s</a>
        se convirtió en:
        <a href="%(baseurl)s%(short)s">%(baseurl)s%(short)s</a>'''%data

        # Clase CSS que muestra las cosas como buenas
        data['clasemensaje']='success'
    else:
        # No se acortó nada, no hay nada para mostrar.
        data['url']=None
        data['short']=None
        data['mensaje']=None

    # Lista de atajos del usuario.
    data ['atajos'] = Atajo.get (user = usuario)

    # Crear la página con esos datos.
    return data

@bottle.route('/:slug/edit')
@bottle.view('atajo.tpl')
def editar(slug):
    """Edita un slug"""
    if not 'REMOTE_USER' in bottle.request.environ:
        bottle.abort(401, "Sorry, access denied.")
    usuario = bottle.request.environ['REMOTE_USER'].decode('utf8')

    # Solo el dueño de un atajo puede editarlo
    a = Atajo.get(slug)
    # Atajo no existe o no sos el dueño
    if not a or a.user != usuario:
        bottle.abort(404, 'El atajo no existe')

    if 'url' in bottle.request.GET:
        # El usuario mandó el form
        a.url = bottle.request.GET['url'].decode('utf-8')
        a.activo = 'activo' in bottle.request.GET
        a.test = bottle.request.GET['test'].decode('utf-8')
        a.save()
        bottle.redirect('/')
        
    return {'atajo':a,
            'mensaje':'',
            }

@bottle.route('/:slug/del')
def borrar(slug):
    """Elimina un slug"""
    if not 'REMOTE_USER' in bottle.request.environ:
        bottle.abort(401, "Sorry, access denied.")
    usuario = bottle.request.environ['REMOTE_USER'].decode('utf8')
    
    # Solo el dueño de un atajo puede borrarlo
    a = Atajo.get(slug)
    if a and a.user == usuario:
        a.delete()
    # FIXME: pasar un mensaje en la sesión
    bottle.redirect('/')

@bottle.route('/:slug/test')
def run_test(slug):
    """Corre el test correspondiente a un atajo"""
    if not 'REMOTE_USER' in bottle.request.environ:
        bottle.abort(401, "Sorry, access denied.")
    usuario = bottle.request.environ['REMOTE_USER'].decode('utf8')

    # Solo el dueño de un atajo puede probarlo
    a = Atajo.get(slug)
    if a and a.user == usuario:
        a.run_test()
    # FIXME: pasar un mensaje en la sesión
    bottle.redirect('/')

# Un slug está formado sólo por estos caracteres
@bottle.route('/(?P<slug>[a-zA-Z0-9]+)')
def redir(slug):
    """Redirigir un slug"""

    # Buscamos el atajo correspondiente
    a = Atajo.get(slug=slug)
    if not a:
        bottle.abort(404, 'El atajo no existe')
    bottle.redirect(a.url)

# Lo de /:filename es para favicon.ico :-)
@bottle.route('/:filename')
@bottle.route('/static/:filename')
def static_file(filename):
    """Archivos estáticos (CSS etc)"""
    bottle.send_file(filename, root='./static/')


if __name__=='__main__':
    """Ejecutar con el server de debug de bottle"""
    bottle.debug(True)
    app = bottle.default_app()

    # Mostrar excepciones mientras desarrollamos
    app.catchall = False

    app = middleware(app,
        enable=True,
        setup_method='openid',
        openid_store_type='file',
        openid_template_file=os.path.join(os.getcwd(),
        'views','invitado.tpl'),
        openid_store_config=os.getcwd(),
        openid_path_signedin='/')

    app = AuthTKTMiddleware(SessionMiddleware(app),
                        'some auth ticket secret');

    # Inicializar DB
    Atajo.init_db()

    # Ejecutar aplicación
    bottle.run(app)
