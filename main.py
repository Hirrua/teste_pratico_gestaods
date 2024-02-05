import pandas
import mysql.connector
from mysql.connector import Error

def formatar_cpf(cpf):
    #aplica mascara no cpf, pego o primeiro elemento até o terceiro, do terceiro até o sexto...
    return f'{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}'

def formatar_cep(cep):
    #aplica mascara no cep
    return f'{cep[:5]}-{cep[5:]}'

def formatar_convenio(convenio):
    #verifica os campos vazios
    if convenio == '':
        return 'Não informado'
    return convenio

def formatar_num_convenio(numero):
    #verifica os campos vazios
    if numero == '':
        return 'Não informado'
    return numero

def formatar_gereno(genero):
    #verifica os generos
    if genero == 'Feminino':
        return 'F'
    elif genero == 'Masculino':
        return 'M'
    elif genero == '':
        return 'N'
    return genero

def formatar_estado(estado):
    #verifica os estados
    if estado == 'Santa Catarina':
        return 'SC'
    elif estado == 'Rio de Janeiro':
        return 'RJ'
    elif estado == 'Distrito Federal':
        return 'DF'
    elif estado == 'São Paulo':
        return 'SP'
    return estado

try:
    #conexão com o banco
    db_con = mysql.connector.connect(host = 'localhost', database = 'backup', user = 'root', password = 'root')

    #verificar se está conectado
    db_info = db_con.get_server_info()
    print('Conectado', db_info)

    #DQL
    query = '''SELECT pacientes.*, 
                    contatos.telefone_fixo, 
                    contatos.celular, 
                    contatos.email, 
                    enderecos.endereco, 
                    enderecos.cidade, 
                    enderecos.bairro, 
                    enderecos.cep, 
                    enderecos.estado, 
                    enderecos.numero
                    FROM pacientes
                    LEFT JOIN contatos ON contatos.paciente_id = pacientes.id_paciente
                    LEFT JOIN enderecos	ON enderecos.paciente_id = pacientes.id_paciente;'''
    
    #cursor para interagir com o banco
    cursor = db_con.cursor()
    
    #executo a query e logo em seguida busco todos os dados e salvo em uma variavel
    cursor.execute(query)
    db_dados = cursor.fetchall()

    #percorro a primeira coluna (0) para pegar o nome das colunas 
    colunas = []
    for coluna in cursor.description:
        colunas.append(coluna[0])

    #transformo os dados em dataframe informando o devido nome para as colunas
    data_frame = pandas.DataFrame(db_dados, columns = colunas)

    #.apply percorre cada linha, chamando a função e definindo o valor
    data_frame['cpf'] = data_frame['cpf'].apply(formatar_cpf)
    data_frame['cep'] = data_frame['cep'].apply(formatar_cep)
    data_frame['estado'] = data_frame['estado'].apply(formatar_estado)
    data_frame['genero'] = data_frame['genero'].apply(formatar_gereno)
    data_frame['convenio'] = data_frame['convenio'].apply(formatar_convenio)
    data_frame['numero_convenio'] = data_frame['numero_convenio'].apply(formatar_num_convenio)

    data_frame['nascimento'] = pandas.to_datetime(data_frame['nascimento'])
    data_frame['nascimento'] = data_frame['nascimento'].dt.strftime('%d-%m-%Y')

    data_frame.to_csv('backup_csv', index=False, encoding='latin-1')

except Error as err:
    #controle caso ocorra algum erro
    print('Não foi possível conectar ao banco')

finally:
    #fecho as conexões
    cursor.close()
    db_con.close()
    print('Concexão fechada')
