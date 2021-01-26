/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package profitparregion;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 *
 * @author hadoop
 */
public class ModeleMapReduceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    // allocation mémoire de la clé et valeur de sortie
    private Text cleS;
    private IntWritable valeurS = new IntWritable();
@Override
    public void reduce(Text cleI, Iterable<IntWritable> valeursI, Context context)
            throws IOException, InterruptedException
    {
        // définir la clé de sortie
        cleS = cleI;
// TODO calculer la valeur de sortie
        int resultat = 0;
        for (IntWritable valeurI : valeursI) {
            int val = valeurI.get();
            resultat = resultat+val;
        }
        valeurS.set(resultat);
// émettre une paire (clé, valeur)
        context.write(cleS, valeurS);
    }
    
    
}
