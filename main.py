import os
import requests
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from openai import OpenAI

# --- CONFIGURACIÓN ---

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
POWER_AUTOMATE_URL = os.getenv("POWER_AUTOMATE_URL")

client = OpenAI(api_key=OPENAI_API_KEY)

estructura_modelo = """
Tablas y columnas disponibles en el modelo semántico:

Calendario: Año, Trimestre, FECHA, MES_CORTO, NOMBRE_DIA_SEMANA, NOMBRE_DIA_SEMANA_CORTO, DIA_SEMANA, AÑO_MES, SEMESTRE, FERIADO, DIA_HABIL, DiaMes, MES_NUM, MES_LARGO, SEMANA_AÑO_NUM, AÑO_MES_NUM, DIA_HABIL_S/N, DIAS_HABILES_MES, DIA_NUM, AñoActual, MesActual, ORDEN_FECHA, DIA_HABIL2, DIA_HABIL_1/0
Actualizacion: UltimaActualizacion
Venta_Historica: FECHA, ID_PRODUCTO, MARCA, ID_SUCURSAL, RUT_CLIENTE, TIPO_DOC, DESCRIP_DOC, NUMERO_PED, DOCUMENTO_REFERENCIA, DIGITADOR, CANTIDAD, TOTAL_NETO, PrimeraCompraB2B, NUMERO_DOC
Metas_Generales: FECHA, Area, Meta
Metas_Comisiones: FECHA, Meta, Nombre Vendedor, TipoVendedor, Atributo
Metas_Concursos: FECHA, Meta, Vendedores, Concurso
Ventagrama_Metas: FECHA, Meta, Concurso
Pedidos Detalle RapiQ: FECHA, DIGITADOR, ID PEDIDO, FECHA HORA PEDIDO, ID SUCURSAL ORACLE, ID PRODUCTO, PACK, CANTIDAD PEDIDA, CANTIDAD FORZADA, PRECIO UNITARIO
Metas_Concursos_Trimestrales: FECHA, Meta, Vendedores, Concurso, Equipo
Concurso_Negocios_Volumen: FECHA, ID_SUCURSAL, NUMERO_PED, MARCA_MODIFICADA, Monto, VENDEDOR TERRENO, JEFATURA
Metas_Clientes_Clave: FECHA, Meta, Rut, NOMBRE, Tipo, JEFE
Guias Controlado: FECHA, CON_CODIGO, TIPR_NOMBRE, CON_FECHA, CON_USUARIO, PED_NUMERO, CON_ESTADO, CON_USUARIO_MODIFICA, CON_FECHA_MODIFICA, COMO_NOMBRE, DES_NOMBRE, DES_DIRECCION, CIU_NOMBRE, COM_NOMBRE, DIR_FANTASIA, DIS_FACTURA, SUC_SECUENCIA, CLI_RUT, NUEVO_DIRECTOR_TECNICO
Concursos_B2B: FECHA, DESCRIPCION, Meta, CODIGO
Metas_Promotor_B2B: FECHA, Meta, Vendedores
PACK FACTURACION: FECHA, ID_PRODUCTO, ID_SUCURSAL, TIPO_DOC, NUMERO_PED, DIGITADOR, CANTIDAD, TOTAL_NETO, NUMERO_DOC, CANTIDAD PEDIDA, CANTIDAD FORZADA, Fecha Pedido, Codigo Pack
Capacitaciones promotor B2B: FECHA, Rut, VENDEDOR TERRENO, Promotor, N° Sucursal, Nombre Sucursal
Maestra_Articulos_DW: ID_PRODUCTO, DESCRIPCION, CATEGORIA_INVENTARIO, LABORATORIO, TIPO_UBICACION, MARCA, PRINCIPIO_ACTIVO, FORMA_FARMACEUTICA, BIOEQUIVALENTE, CLASE_COMERCIAL, CONTROLADO, DOSIS, PETITORIO_MINIMO, POSICIONAMIENTO, PESO, ALTO, ANCHO, LARGO, VOLUMEN, NUMERO_REGISTRO, BLOQUEO_VENTA, ESTADO, COMPRADOR, POLITICA_CANJE, TIPO_FORMULACION, INFLAMABLE, SUJETO_CONTROL, PRINCIPIO_ACTIVO2, ENVASE, ACCION_TERAPEUTICA, CLASE_TERAPEUTICA, AREA_NEGOCIO, CATEGORIA_COMERCIAL, FORMA_FARMACEUTICA_2, MARCA_COMERCIAL, FECHA_CREACION, PRODUCTO, CATEGORIA_COMPRA, UBICACION_BODEGA, GENERICO_MARCA, CODIGO_MADRE, SUB_AREA_NEGOCIO, Stock, Fecha Ultima Venta, BLOQUEO_VENTAS, PRESENTACION, PRODUCTO_CORTO, ID_PRODUCTO_WMS, Ean13
Concursos_Invierno: DESCRIPCION, MARCA, CODIGO, OFERTA INVIERNO, P.FINAL
Maestra_Clientes_DW: CLASE_COMERCIAL, BLOQUEO_VENTA, FECHA_CREACION, RAZON_SOCIAL, NOMBRE_FANTASIA, GIRO_COMERCIAL, TIPO_CLIENTE, CLIENTE_ESPECIAL, VENCIMIENTO<ANIO, CONDICION_PAGO, LIMITE_CREDITO, NOMBRE_CONTACTO, TELEFONO_CONTACTO, EMAIL_CONTACTO, REPRESENTANTE_LEGAL, RUT_REP_LEGAL, DIRECTOR_TECNICO, GIRADOR, RUT_GIRADOR, ID_SUCURSAL, NOMBRE_SUCURSAL, RUTA, DT_VETERINARIA, LISTA_PRECIOS, COMPRA_CENTRALIZADA, DIRECCION, CIUDAD, COMUNA, PROVINCIA, REGION, PAIS, FACTURA, ENVIO, VENDEDOR_TERRENO, VENDEDOR_TLMK, VENDEDOR_ANC, VENDEDOR_NATURAL, COMENTARIO1, COMENTARIO2, FECHA_TERMINO, RUT_DT, RUT_CLIENTE, STATUS, FECHA_ULTIMA_VENTA, CLIENTES ACTIVOS NO VENTA +3M, AGRUPACION_ULTIMA_VENTA, COMUNA_2, PROVINCIA_2, REGION_2, ZONA_GEOGRAFICA, Clase Comercial2, SUCURSALES ACTIVAS, FECHA B2B, ORDEN ZONAS_GEOGRAFICAS, Kam_Vet, Latitud, Longitud, Status Compra, PromotorB2B, FECHA_CREACION_SUCURSAL, Meta General, Meta Veterinaria, Meta Perfumeria, Jefe_Ventas, ID_SUCURSAL_WMS, ZONA GEOGRAFICA, CODIGO_REGION, Cuadrante, Clasificación, CAT COMPRA NN
Stock General Ravepol: Stock, PRO_PRODUCTO
Comunas_Geograficas: COMUNA, PROVINCIA, Latitud, Longitud, Orden, Orden2, CUT (Código Único Territorial), Densidad(hab./km2), IDH 2005, IDH 2005_1, Población2017, Región, Superficie(km2), Zonas
Ruta Promotores B2B: COMUNA, Promotor
Sucursales Objetivos FreshSales: COMUNA, ID, VENDEDOR TERRENO, CLASE COMERCIAL, Vendedor, Codigo de Sucursal, Nombre de la Sucursal, Rut Cliente/Empresa, B2B, Correo electrónico del propietario
Ruta Promotores B2B( comunas base): COMUNA, Promotor
Clientes_Tramo: RUT_CLIENTE
RUTs B2B MesAnterior: RUT_CLIENTE, Compró B2B Este Mes
Destinatario_Direccion: Latitud, Longitud, DIR_SUCURSAL
Vendedor_Digitador: DIGITADOR, Equipo, Canal, Nombre Digitador
AreaNegocio: Area, Orden
ZonaVendedores: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Vend_Terreno: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Vend_ANC: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Vend_ANC-TLMK: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Vend_Asistente: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Kam_Farma: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Vend_Tlmk: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Vend_PromotorB2B: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Vend_Kam-Vet: Vendedores, Zona, Orden, Equipo, Zona1, Orden2, Ejecutivo Cobranzas, Rut, ClaveUsuario2, Correo, NombreDigitacion, Jefe Ventas, Canal
Metas_Concursos_Adicionales: Concurso, Equipo, CODIGO, Descrpcion, LAB, VIGENCIA, Multiplos
ConcursosClientes: Concurso, CODIGO, VIGENCIA
Ventagrama: Orden, Ventagrama
Clientes_TramoSInNC: Rut, NOMBRE, Columna1, Columna2, Columna3
ClientesVet_Metas: Rut, 1, 2, 3
Clientes ANCCondicionesEspeciales: Rut, Condiciones
ANC Clientes_Incentivo: Rut, Tipo Incentivo
ClientesVet: Rut
ClientesVet_Descuento: Rut, 1, 2, 3
Kam_Vet: Rut, Vendedor
Transacciones promotor B2B: Rut, VENDEDOR TERRENO, Promotor, N° Sucursal, Nombre Sucursal, Fecha transacción, Programa
Tipo De Venta: ID, TipoVenta
Visitas efectivas Freshsales: ID, VENDEDOR TERRENO, Vendedor, Codigo de Sucursal, Nombre de la Sucursal, Correo electrónico del propietario, País, Sector/Industria, Tipo de Cliente, Actividad de ventas Checkedout At, Actividad de ventas Checkedout At - Copia
Contraseña_KamFarma: Contraseña_KamFarma
Contraseña_ANC-TLMK: Contraseña_ANC-TLMK
Contraseña_ASISTENTE: Contraseña_ASISTENTE
Contraseña_Terreno: Contraseña_Terreno
Contraseña_TLMK: Contraseña_TLMK
ANC Incentivo1: CODIGO, DESCRIPCIÓN, Incentivo
ANC Incentivo2: CODIGO, DESCRIPCIÓN, Incentivo
Tipo Clientes B2B: CLASE COMERCIAL, Tipo Cliente
Guia Despacho: PRO_PRODUCTO, GUIA_FOLIO, GUIA_FECHA, GUIA_USUARIO, GUIA_TIPO, GUIA_TRASLADO, GUIA_MOTIVO, GUIA_GLOSA_1, GUIA_GLOSA_2, GUIA_GLOSA_3, GUIA_EQUIPO, GUIA_NETO, BOD_CODIGO, GUIA_BIEN, GUIA_ESTADO, GUIA_RUT, GUIA_NOMBRE, GUIA_DIRECCION, GUIA_COMUNA, GUIA_CIUDAD, GUIA_REGION, GUIA_TELEFONO, AJU_CODIGO, GUIA_MOTIVO_NOMBRE, GUIA_TRASLADO_NOMBRE, NOMBRE_DESPACHO, GUIA_BIEN_NOMBRE, GUIADET_FOLIO, PRO_CODIGO, PRO_NOMBRE, GUIADET_CANTIDAD, GUIADET_PRECIO, GUIADET_LOTE, GUIADET_VENCIMIENTO, EST_CODIGO, UBI_CODIGO, BOD_NOMBRE, EST_NOMBRE, GUIA_TIPO_SII, GUIA_COMERCIAL, CIU_ERP, CIU_IATA, GUIA_TRASLADO_SII, FECHA-HORA
Contactos Fuerza de Venta: Vendedor, Codigo de Sucursal, Estado de la Sucursal, Nombre de la Sucursal, Last contacted time
Metas_Clientes_Veterinaria: NUMERO_SUC, Meta_Cliente
Metas_Clientes_Perfumeria: NUMERO_SUC, Meta_Cliente
Metas_Clientes_General: NUMERO_SUC, Meta_Cliente
Contraseña_KAM-VET: Contraseña_KAM-VET

"""

# --- FUNCIONES ---

def pregunta_a_dax(pregunta):
    prompt = f"""
        Eres un asistente experto en análisis de datos y lenguaje DAX, que ayuda a generar consultas precisas y ejecutables para Power BI.

        Dispones de un modelo de datos con las siguientes tablas y columnas:

        {estructura_modelo}

        Tu tarea es interpretar cualquier pregunta hecha en lenguaje natural por un usuario y generar una consulta DAX correcta, optimizada y ejecutable. Siempre usa los nombres exactos de tablas y columnas proporcionados.

        📌 📌 Consideraciones clave para generar la consulta DAX:
        - Usa funciones como `SUM`, `MAX`, `COUNTROWS`, `CALCULATE`, `FILTER`, `SUMMARIZECOLUMNS`, etc., según la intención de la pregunta.
        - Si necesitas filtrar por fechas relativas (como "mes pasado", "últimos 7 días"), **usa siempre columnas de tipo `Date` (como `'Calendario'[FECHA]` o `'Calendario'[ORDEN_FECHA]`) para funciones como `EDATE`, `TODAY()`, `DATEADD`**.  
        **Nunca compares columnas tipo texto o número (como `'Calendario'[AÑO_MES_NUM]`) directamente con resultados de esas funciones.**  
        Si debes usar `AÑO_MES_NUM`, calcula el valor numérico correspondiente al periodo deseado con una operación aritmética, como `MAX('Calendario'[AÑO_MES_NUM]) - 1`.

        Ejemplo incorrecto:

        FILTER(ALL('Calendario'),'Calendario'[AÑO_MES_NUM] = EDATE(MAX('Calendario'[AÑO_MES_NUM]),-1))

        Ejemplo correcto:

        FILTER(ALL('Calendario'), 'Calendario'[FECHA] >= EDATE(TODAY(),-1) && 'Calendario'[FECHA] <= TODAY())

        o bien:

        VAR MesAnterior = MAX('Calendario'[AÑO_MES_NUM]) - 1
        FILTER(ALL('Calendario'), 'Calendario'[AÑO_MES_NUM] = MesAnterior)


        - Si el usuario se refiere a:
        - "vendedor" → usa las tablas `'Vend_PromotorB2B', 'Vend_Terreno','Vend_Tlmk'`
        - "nombre del médico" → usa la columna `'Nombre Medico'`
        - "ventas", "precio", etc. → revisa la tabla `'Venta_Historica'`
        - "productos" → usa `'Maestra_Articulos_DW'`
        - "detalle del stock producto" → usa `'Stock General Ravepol'[stock]`
        - "precio total" → usa `'Venta_Historica'[TOTAL_NETO]`
        - "fecha de venta" → usa la columna `'FECHA'` de la tabla correspondiente


        ✅ Cuando uses `SELECTCOLUMNS(...)` seguido de `FILTER(...)`, recuerda que:
        - Solo puedes referenciar las columnas renombradas directamente por su alias (por ejemplo: `[Producto]`, no `'Maestra_Productos'[Producto]`).
        - Alternativamente, usa `CALCULATETABLE(...)` para aplicar el filtro antes de seleccionar columnas.

        ✅ Cuando el usuario solicite un **único valor agregado** (por ejemplo, el total de ventas del mes pasado o la suma general):
        - Utiliza `EVALUATE ROW(...)` o `EVALUATE { ... }` para devolver solo una fila con una etiqueta descriptiva.
        - Evita `SUMMARIZECOLUMNS` en estos casos para no devolver múltiples filas.
        - Asegúrate de que la consulta sea ejecutable directamente en Power BI.

        🛑 No uses `VAR` ni `RETURN`, ya que la consulta debe ser directamente ejecutable como una sentencia `EVALUATE`.

        Consulta del usuario:
        \"{pregunta}\"

        Devuelve únicamente la consulta DAX completa y válida, sin comentarios ni explicación.
        """


    response = client.chat.completions.create(
        model="gpt-4.1",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content.strip()


def enviar_a_power_automate(dax_query):
    body = {"query": dax_query}
    headers = {"Content-Type": "application/json"}

    response = requests.post(POWER_AUTOMATE_URL, headers=headers, json=body)

    if response.status_code == 200:
        try:
            json_respuesta = response.json()
            resultado = json_respuesta.get("resultado", "Sin resultado desde Power Automate")
            return resultado
        except Exception:
            return "⚠️ Error leyendo la respuesta JSON"
    else:
        return f"❌ Error en Power Automate: {response.status_code}"


def enviar_mensaje_telegram(chat_id, mensaje):
    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": mensaje}
    requests.post(url, json=payload)

def formatear_respuesta(resultado):
    if resultado is None:
        return "⚠️ No se encontraron resultados para tu consulta."

    # Si es lista
    if isinstance(resultado, list):
        if len(resultado) == 0:
            return "⚠️ No se encontraron resultados para tu consulta."

        # Si es una lista con un único diccionario con una única clave, devuelve solo su valor
        if len(resultado) == 1 and isinstance(resultado[0], dict) and len(resultado[0]) == 1:
            return list(resultado[0].values())[0]

        # Si es una lista de diccionarios o valores, listarlos todos
        mensajes = []
        for item in resultado:
            if isinstance(item, dict):
                if len(item) == 1:
                    mensajes.append(f"{list(item.values())[0]}")
                else:
                    for clave, valor in item.items():
                        mensajes.append(f"🔹 {clave}: {valor}")
            else:
                mensajes.append(str(item))
        return "\n".join(mensajes)

    # Si es diccionario
    if isinstance(resultado, dict):
        if len(resultado) == 1:
            return list(resultado.values())[0]
        mensajes = [f"🔹 {clave}: {valor}" for clave, valor in resultado.items()]
        return "\n".join(mensajes)

    # Si es string
    if isinstance(resultado, str):
        if resultado.strip() == "":
            return "⚠️ No se encontraron resultados para tu consulta."

        if "error" in resultado.lower() or "exception" in resultado.lower():
            return "❌ Hubo un problema al procesar la consulta. Por favor, revisa tu pregunta."

        if resultado.strip().startswith("{") and resultado.strip().endswith("}"):
            try:
                data = eval(resultado)
                if isinstance(data, dict):
                    if len(data) == 1:
                        return list(data.values())[0]
                    mensajes = [f"🔹 {clave}: {valor}" for clave, valor in data.items()]
                    return "\n".join(mensajes)
            except Exception:
                pass

        return f"📈 Resultado obtenido:\n{resultado}"

    return str(resultado)


# --- FASTAPI APP ---

app = FastAPI()

@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    message = data.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    texto = message.get("text", "")

    if not chat_id or not texto:
        return JSONResponse(content={"ok": True})

    dax = pregunta_a_dax(texto)
    resultado_bruto = enviar_a_power_automate(dax)
    resultado_formateado = formatear_respuesta(resultado_bruto)

    respuesta = f"📊 Tu consulta fue:\n{texto}\n\n{resultado_formateado}"
    enviar_mensaje_telegram(chat_id, respuesta)

    return JSONResponse(content={"ok": True})



 