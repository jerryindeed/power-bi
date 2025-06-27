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

Actualizacion: UltimaActualizacion
Calendario: Año, DateKey, TrimestreNumero, TrimNom, NumeroMes, NombreMes, NombreMesCorto, NumeroSemanaAño, DiaNumeroMes, NombreDiaSemana, NombreDiaSemanaCorto, AñoMes, YM, MesCalendario, DiaSemNum, AñoActual, MesActual, feriado, DiaHabil
Data Ventas: Año, Folio, NombreMedico, NombreCliente, FechaCreacion, detalle, Cantidad, frascos, DetalleProducto, DosisProducto, UnidadMedida, ValorPreparado, PrecioCosto, año_fecha, mes_fecha, idMedico, idCliente, idProducto, idFormaFarma, DIA_SEMANA, Dosis gramos, Consumo g, PrecioCostoUnitario, ValorEtiqueta, ValorEnvase, ValorMaterialEmpaque, ValorFrasco, ValorCapsulas, ValorCostoTotal, ValorLactosa, ValorTalco
Maestra_Cliente: NombreCliente, RUT, DIGITO, NOMBRE, PATERNO, MATERNO, DIRECCION, TELEFONO, CELULAR, FAX, CIUDAD, COMUNA, REGION, REGION_CORTA, fechanacimiento, fechaModificacion, RutCliente, REGION2
Maestra_Medicos: idMedico, RUT, DIGITO, NOMBRE, PATERNO, MATERNO, DIRECCION, TELEFONO, CELULAR, FAX, CIUDAD, COMUNA, REGION, REGION_CORTA, fechaModificacion, observaciones, valorAdicional, mail, idRepresentante, idUsuarioCreacion, idUsuarioModificacion, Nombre Medico, Rut Medico, Especialidad, idEspecialidad
Maestra_Productos: idProducto, FechaCreacion, detalle, UnidadMedida, fechaModificacion, codigo_ISP, precio_venta, precio_costo, controlado, dosismaxima, codigobase, sinonimo, idUsuarioCreado, precioUnico, equivalencia, unidadMedidaEquivalencia, stock, stockCritico, maximoCapsula, tipoMateriaPrima, Venta_Publico, Precio Venta mg, Precio Costo mg, Activo, diasVencimiento
Maestra_Producto_Lote: idProducto, idProductoLote, lote, FechaVencimientoLote, Activo
Maestra_Forma_Farmaceutica: detalle, fechaModificacion, idFormaFarma, precioBase, tipoCalculo, Activo, unidad, diasVencimiento, horasEntrega
Maestra_Proveedor: RUT, DIGITO, NOMBRE, DIRECCION, TELEFONO, CELULAR, FAX, CIUDAD, razon_social, idCiudad, idComuna, idProveedor
Agrupacion Cantidades por Medico: idMedico, Folio, Cantidad, frascos, FechaCreacion
Feriados: Fecha Feriado
"""

# --- FUNCIONES ---

def pregunta_a_dax(pregunta):
    prompt = f"""
    Eres un asistente experto en análisis de datos y lenguaje DAX, que ayuda a generar consultas precisas y ejecutables para Power BI.

    Dispones de un modelo de datos con las siguientes tablas y columnas:

    {estructura_modelo}

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
    resultado = enviar_a_power_automate(dax)

    respuesta = f"📊 Consulta:\n{texto}\n\n✅ Resultado:\n{resultado}"
    enviar_mensaje_telegram(chat_id, respuesta)

    return JSONResponse(content={"ok": True})
