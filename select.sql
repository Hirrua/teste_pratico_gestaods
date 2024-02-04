SELECT pacientes.*, 
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
LEFT JOIN enderecos	ON enderecos.paciente_id = pacientes.id_paciente;