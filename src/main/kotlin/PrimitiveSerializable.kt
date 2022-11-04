import java.io.DataOutputStream

interface PrimitiveSerializable {
    fun write(dataOutputStream: DataOutputStream)
}
