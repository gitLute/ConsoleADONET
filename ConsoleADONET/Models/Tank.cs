namespace ConsoleADONET.Models
{
    public class Tank
    {
        public int    TankId       { get; set; }
        public string TankType     { get; set; }
        public float  TankVolume   { get; set; }
        public float  TankWeight   { get; set; }
        public string TankMaterial { get; set; }

        public override string ToString() =>
            $"{TankId,-8}{TankType,-20}{TankVolume,-12:F2}{TankWeight,-12:F2}{TankMaterial}";
    }
}
