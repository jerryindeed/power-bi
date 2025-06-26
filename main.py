from openai import OpenAI
import requests
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
import uvicorn

# --- CONFIGURACIÓN ---

# Tu API key de OpenAI
OPENAI_API_KEY = "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"
# URL de Power Automate
POWER_AUTOMATE_URL = "https://prod-40.westus.logic.azure.com:443/workflows/bf72ea2d7282488da107a2115535502b/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=2aVJlesHOqqwyeVv7jPfGyTAUYx2U34dvXMh4dXKZUM"

# Cliente OpenAI
client = OpenAI(api_key=OPENAI_API_KEY)

# Estructura del modelo semántico
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

# --- API FASTAPI ---

app = FastAPI()

@app.post("/whatsapp-webhook")
async def whatsapp_webhook(request: Request):
    data = await request.form()
    mensaje = data.get("Body")  # este es el nombre del campo de Twilio para el mensaje recibido

    if not mensaje:
        return PlainTextResponse("No se recibió mensaje", status_code=400)

    # Procesamos la consulta
    dax = pregunta_a_dax(mensaje)
    resultado = enviar_a_power_automate(dax)

    # Devolvemos la respuesta que Twilio reenvía al usuario por WhatsApp
    respuesta = f"📊 Consulta:\n{mensaje}\n\n✅ Resultado:\n{resultado}"
    return PlainTextResponse(respuesta)

# --- INICIO LOCAL ---
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
