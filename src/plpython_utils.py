import inspect

import pandas.io.sql as psql

def parametrized(dec):
    def layer(*args, **kwargs):
        """
        Wrapper for creating plpython functions. Argument types are provided in the docstring

        :param conn: Psycopg connection object
        :param schema: Schema name to create decorator
        :param return_type: Postgres return type

        Example
        ~~~~~~~
        @plpython(conn, 'wnv','int')
        def my_func(a,b):
            '''float, int'''
            return a + b
        """
        def repl(f):
            return dec(f, *args, **kwargs)
        return repl
    return layer

def arg_parser(arg_vals, arg_types):
    j = ['{} {}'.format(v,t) for v,t in zip(arg_vals, arg_types)]
    return ', '.join(j)

@parametrized
def plpython(f, conn, schema, return_type):
    """See above"""
    arg_names = inspect.getargspec(f).args
    def aux(f, conn, schema, return_dtype, arg_names):
        try:
            dtypes = f.__doc__.split(',')
        except AttributeError:
            raise Exception("You must supply argument types in the docstring")
        arg_def = arg_parser(arg_names, dtypes)
        lines = inspect.getsourcelines(f)[0][3:]
        fxn_code = ''.join(lines)
        fxn_name = f.__name__
        params = {'schema': schema,
                  'fxn_name': f.__name__,
                  'arg_def': arg_def,
                  'return_type': return_type,
                  'fxn_code': fxn_code}

        sql = '''
DROP FUNCTION IF EXISTS {schema}.{fxn_name} ({arg_def});
CREATE OR REPLACE FUNCTION {schema}.{fxn_name} ({arg_def})
RETURNS {return_type}
AS $$
{fxn_code}
$$ LANGUAGE plpythonu;
        '''.format(**params)
        psql.execute(sql, conn)
        print "Successfully created function: {schema}.{fxn_name}({arg_def})".format(**params)
    return aux(f, conn, schema, return_type, arg_names)
