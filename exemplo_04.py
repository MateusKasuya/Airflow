def minha_funcao_decoradora(funcao_decorada):
    def funcao_wrapper():
        print("essa aqui vai ser um decorador")
        funcao_decorada()
    return funcao_wrapper

@minha_funcao_decoradora
def minha_funcao():
    print("um print")

minha_funcao()
    