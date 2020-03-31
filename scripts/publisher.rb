require "google/cloud/pubsub"
require "time"

pubsub = Google::Cloud::Pubsub.new project: '<PROJECT-ID>'
topic_name = '<TOPIC_NAME>'
topic = pubsub.topic(topic_name)

def create_example(name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'CREATE', data: {nome: name, cargo: job}}
end

def create_example_without_type(name, job)
  {timestamp: Time.now.utc.iso8601(3), data: {nome: name, cargo: job}}
end

def create_example_invalid_type(name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'BATMAN', data: {nome: name, cargo: job}}
end

def create_example_without_timestamp(name, job)
  {type: 'CREATE', data: {nome: name, cargo: job}}
end

def create_example_without_data(name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'CREATE'}
end

def create_example_without_nome(name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'CREATE', data: {cargo: job}}
end

def create_example_without_cargo(name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'CREATE', data: {nome: name}}
end

def update_example(id, name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'UPDATE', data: {id: id, nome: name, cargo: job}}
end

def update_example_without_type(id, name, job)
  {timestamp: Time.now.utc.iso8601(3), data: {id: id, nome: name, cargo: job}}
end

def update_example_invalid_type(id, name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'CORINGA', data: {id: id, nome: name, cargo: job}}
end

def update_example_without_timestamp(id, name, job)
  {type: 'UPDATE', data: {id: id, nome: name, cargo: job}}
end

def update_example_without_data(id, name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'UPDATE'}
end

def update_example_without_id(id, name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'UPDATE', data: {nome: name, cargo: job}}
end

def update_example_without_nome(id, name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'UPDATE', data: {id: id, cargo: job}}
end

def update_example_without_cargo(id, name, job)
  {timestamp: Time.now.utc.iso8601(3), type: 'UPDATE', data: {id: id, nome: name, }}
end

def delete_example(id)
  {timestamp: Time.now.utc.iso8601(3), type: 'DELETE', data: {id: id}}
end

def delete_example_without_type(id)
  {timestamp: Time.now.utc.iso8601(3), data: {id: id}}
end

def delete_example_invalid_type(id)
  {timestamp: Time.now.utc.iso8601(3), type: 'ROBIN', data: {id: id}}
end

def delete_example_without_timestamp(id)
  {type: 'DELETE', data: {id: id}}
end

def delete_example_without_data(id)
  {timestamp: Time.now.utc.iso8601(3), type: 'DELETE'}
end

def delete_example_without_id(id)
  {timestamp: Time.now.utc.iso8601(3), type: 'DELETE', data: {}}
end

def first_name
  names = ['Luiz', 'André', 'Felipe', 'Eduardo', 'Davi', 'Eduardo', 'Gabriel', 'João', 'João', 'Julio', 'Fernando', 'Marcos', 'Paulo', 'Pedro', 'Tales', 'Vinícios', 'Victor', 'Arthur', 'Ulisses', 'Davi', 'Éder', 'Eugênio', 'Miguel', 'Otávio', 'Plínio', 'Bernardo', 'Rodrigo', 'Danilo', 'Fabrício', 'George', 'Heitor', 'Otto', 'João', 'Vinícios', 'Erick']
  names[rand(names.size)]
end

def last_name
  names = ['ABREU', 'ALBUQUERQUE', 'ALMEIDA', 'AMADEU', 'ANTUNES', 'ASSUNÇÃO', 'Augusto', 'AZEVEDO', 'BARCELOS', 'BARROS', 'CABRAL', 'C NDIDO', 'CARVALHO', 'COELHO', 'COSTA', 'COUTINHO', 'DUARTE', 'HERNANDEZ', 'FERRARI', 'GARCIA', 'GOMES', 'GONÇALVES', 'JOHNSON', 'LEVY', 'LINS', 'MATTOS', 'MELLO', 'MENDES', 'MONTEIRO', 'MORAES', 'MOTTA', 'MOURA', 'OTTO', 'PAIVA', 'PEIXOTO', 'PINHEIRO', 'QUEIROZ', 'RANGEL ', 'ROCHA', 'ROSSI', 'SANTOS', 'SILVINO', 'STEFANINI', 'STEVES', 'TORRES', 'VIEIRA', 'WEBER']
  names[rand(names.size)]
end

def job_name
  names = ['Consultor de vendas', 'Auxiliar de limpeza', 'Abastecedor de linha de produção', 'Ajudante de obras', 'Auxiliar administrativo', 'Atendente de pedágio', 'Agente administrativo', 'Caçambeiro', 'Arrumador de prateleiras - em supermercados', 'Agente de tráfego', 'Entijolador', 'Ajudante de churrasqueiro', 'Atendente balconista', 'Ajudante', 'Cozinheiro de restaurante', 'Agregado - na agropecuária', 'Operador de teleatendimento híbrido (telemarketing)', 'Guariteiro', 'Abanador na agricultura', 'Almoxarife', 'Agente de segurança ferroviária', 'Ajudante de carga e descarga de mercadoria', 'Promotor de vendas', 'Técnico de enfermagem', 'Trabalhador de serviços de limpeza e conservação de áreas públicas', 'Atendente de posto de gasolina', 'Atendente de buffet', 'Colhedor de cana-de-açúcar', 'Açougueiro', 'Colhedor de banana', 'Ajudante de embalador', 'Chofer', 'Auxiliar de armazenamento', 'Assistente de vendas', 'Montador soldador', 'Agente administrativo supervisor', 'Auxiliar de conservação de barragens', 'Vigia', 'Ajudante de farmácia', 'Gerente administrativo', 'Vendedor em comércio atacadista', 'Arador', 'Analista de desenvolvimento de sistemas', 'Ajustador de máquinas de embalagem', 'Auxiliar de costura', 'Atendente central telemarketing', 'Assistente de serviço de contabilidade', 'Alinhador de rodas', 'Ajudante de eletricista', 'Zelador', 'Enfermeiro', 'Ajudante de pintor', 'Motofretista', 'Apanhador de café', 'Carapina', 'Condutor de veículo de carga', 'Carregador (armazém)', 'Auxiliar de serviço de copa', 'Chasquil', 'Carregador (veículos de transportes terrestres)', 'Supervisor de segurança do trabalho', 'Costureira de máquina overloque', 'Motorista de ônibus urbano', 'Arrumadeira de hotel', 'Construtor civil', 'Administrador', 'Farmacêutico', 'Encarregado de supermercado', 'Vendedor de consórcio', 'Colocador de estruturas metálicas', 'Motorista de empilhadeira', 'Asfaltador na conservação de vias permanentes (exceto trilhos)', 'Atendente de clínica médica', 'Manipulador de máquinas fixas', 'Auxiliar de manutenção elétrica e hidráulica', 'Açougueiro classificador (exclusive comércio)', 'Ajudante de boiadeiro', 'Eletricista', 'Administrador no comércio de mercadorias', 'Encarregado de padaria', 'Analista de cobrança', 'Supervisor comercial', 'Assistente de prevenção de perdas', 'Conferente de carga e descarga', 'Operador de estufas mecânicas', 'Caldeireiro (chapas de ferro e aço)', 'Operador de teleatendimento ativo (telemarketing)', 'Auxiliar de encanador', 'Agente de inspeção (qualidade)', 'Ajudante de confecção', 'Analista de suporte computacional', 'Ajudante de lavador de automóvel', 'Montador de andaimes (edificações)', 'Gerente de área de vendas', 'Analista de Desenvolvimento Humano Organizacional', 'Atendente de creche', 'Administrador de contadorias e registros fiscais', 'Desenvolvedor Mobile', 'Promotor de vendas especializado', 'Trabalhador polivalente da confecção de calçados', 'Auxiliar de marceneiro', 'Auxiliar de enfermagem', 'Motorista de ônibus rodoviário', 'Assessor de diretoria', 'Analista de logística (técnico de nível médio)', 'Carpinteiro (obras)', 'Ajudante de serralheiro', 'Montador de artefatos de madeira', 'Encarregado de garagem', 'Armador de estrutura de concreto armado', 'Abatedor - na extração de madeira', 'Vendedor pracista', 'Apontador de cartões de ponto', 'Operador de máquina de terraplenagem', 'Agente de vendas de serviços', 'Jardineiro (árvores para ornamentação urbana)', 'Instalador de máquinas', 'Guest relations', 'Gari', 'Alfeloeiro', 'Demonstrador de mercadorias', 'Analista Big Data', 'Atendente comercial (agência postal)', 'Abatedor', 'Auxiliar de garçom', 'Auxiliar de torneiro mecânico', 'Analista de Service Desk', 'Agente de coleta de lixo', 'Analista de BI (Business Intelligence)', 'Anotador de processo de produção', 'Armador de estrutura de concreto', 'Condutor de escavadeira', 'Operador de suporte técnico (telemarketing)', 'Cobrador de transportes coletivos (exceto trem)', 'Arrematador', 'Auxiliar de faturamento', 'Operador de negócios', 'Assistente de engenharia (construção civil)', 'Auxiliar de lavanderia', 'Alvanel', 'Acompanhante de idosos', 'Atendente de clínica dentária', 'Plantador de milho e sorgo - empregador', 'Técnico de Infraestrutura TI', 'Chanfrador de calçados', 'Gesseiro', 'Administrador de refeitório', 'Auxiliar de eletrônica', 'Go - gentil organizador', 'Banguezeiro - empregador', 'Ajudante de estruturas metálicas', 'Adegueiro', 'Avicultor - exclusive conta própria na avicultura de postura', 'Afinador de motores a diesel', 'Calculista de produção', 'Atendente de bar', 'Eletricista de alta-tensão', 'Analista de controle de qualidade', 'Professor das séries iniciais', 'Pintor de estruturas metálicas', 'Ajudante de reparador (telecomunicações)', 'Encarregado de telemarketing', 'Açougueiro cortador (exclusive comércio)', 'Injetor de plástico', 'Educador infantil de nível médio', 'Auxiliar mecânico de ar condicionado', 'Professor de ensino fundamental - séries iniciais', 'Arraçoador (pecuária polivalente)', 'Auxiliar de coordenação de ensino fundamental de primeira a quarta séries', 'Nutricionista', 'Secretária (técnico em secretariado - português)', 'Ajudante de derrubada', 'Operador de centro telefônico', 'Motorista de caminhão-guincho leve', 'Avicultor de corte - exclusive conta própria e empregador', 'Representante técnico de vendas', 'Analista de logística de transporte', 'Instrutor de auto-escola', 'Costureira de peças sob encomenda', 'Médico clínico', 'Escriturário de banco', 'Professor de creche', 'Atendente de agência', 'Operador de centro de processamento de dados', 'Flanelinha', 'Analista de planejamento financeiro', 'Desenhista de editorial', 'Caixa de banco', 'Borracheiro', 'Ajudante de ensacador', 'Assistente de cozinhador', 'Auxiliar de laboratório de análises clínicas', 'Instalador de aparelhos telefônicos', 'Gerente de armazém', 'Colhedor de uva', 'Conservador de linhas elétricas e telefônicas', 'Auxiliar de processamento de fumo', 'Eletricista de instalações', 'Advogado', 'Operador de manufaturado']
  names[rand(names.size)]
end

# publish creates
(0..100).map do |i|
  name = first_name + ' ' + last_name
  job = job_name
  topic.publish(create_example(name, job).to_json)
end

# publish creates with invalid json
(0..10).map do |i|
  name = first_name + ' ' + last_name
  job = job_name
  topic.publish(create_example_without_type(name, job).to_json)
  topic.publish(create_example_invalid_type(name, job).to_json)
  topic.publish(create_example_without_timestamp(name, job).to_json)
  topic.publish(create_example_without_data(name, job).to_json)
  topic.publish(create_example_without_nome(name, job).to_json)
  topic.publish(create_example_without_cargo(name, job).to_json)
end

created_ids = []
# publish updates
(0..20).map do |i|
  id = rand(2_000_000)
  created_ids << id
  name = first_name + ' ' + last_name
  job = job_name
  topic.publish(update_example(id, name, job).to_json)
end

# publish updates with invalid json
(0..20).map do |i|
  id = rand(2_000_000)
  created_ids << id
  name = first_name + ' ' + last_name
  job = job_name
  topic.publish(update_example_without_type(id, name, job).to_json)
  topic.publish(update_example_invalid_type(id, name, job).to_json)
  topic.publish(update_example_without_timestamp(id, name, job).to_json)
  topic.publish(update_example_without_data(id, name, job).to_json)
  topic.publish(update_example_without_id(id, name, job).to_json)
  topic.publish(update_example_without_nome(id, name, job).to_json)
  topic.publish(update_example_without_cargo(id, name, job).to_json)
end

# publish deletes
(0..10).map do |i|
  id = created_ids[rand(created_ids.size)]
  topic.publish(delete_example(id).to_json)
end


# publish deletes with invalid json
(0..10).map do |i|
  id = created_ids[rand(created_ids.size)]
  topic.publish(delete_example_without_type(id).to_json)
  topic.publish(delete_example_invalid_type(id).to_json)
  topic.publish(delete_example_without_timestamp(id).to_json)
  topic.publish(delete_example_without_data(id).to_json)
  topic.publish(delete_example_without_id(id).to_json)
end

# publish invalid data
topic.publish('<xml>I am not a Json</xml>')
topic.publish("[]")
topic.publish("Abcd 123456")
