namespace ConsoleADONET.Models
{
    public class Fuel
    {
        public int    FuelId      { get; set; }
        public string FuelType    { get; set; }
        public float  FuelDensity { get; set; }

        public override string ToString() =>
            $"{FuelId,-8}{FuelType,-20}{FuelDensity:F4}";
    }
}
