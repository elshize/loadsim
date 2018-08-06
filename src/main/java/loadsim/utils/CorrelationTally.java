package loadsim.utils;

import java.util.ArrayList;
import java.util.List;

import desmoj.core.simulator.Model;
import desmoj.core.statistic.Tally;
import desmoj.core.statistic.ValueSupplier;

public class CorrelationTally extends Tally {
  
  private double x2 = 0; 
  private double y2 = 0;
  private double xy = 0;
  private int numObs = 0;
  
  public List<Double> lastThousand = new ArrayList<>();

  public CorrelationTally(Model ownerModel, String name, ValueSupplier valSup,
      boolean showInReport, boolean showInTrace) {
    super(ownerModel, name, valSup, showInReport, showInTrace);
  }
  public CorrelationTally(Model ownerModel, String name, boolean showInReport, boolean showInTrace) {
    super(ownerModel, name, showInReport, showInTrace);
  }

  public void update(double val) {
    super.update(val);
    lastThousand.add(val);
    if (lastThousand.size() > 100) {
      lastThousand.remove(0);
    }
    numObs += 1;
    x2 += Math.pow(numObs,2);
    y2 += Math.pow(val, 2);
    xy += numObs*val;
  }
  
  public double getCorrelation() {
    return xy/Math.sqrt(x2*y2);
  }
 
  public double getSlope() {
    // check to see if the slope is overly large at the end of the run 
    double ystdev = getStdDev();

    int xsize = (int) getObservations();
    double xmean = ((double) xsize) / 2;

    double xstdev = 0;
    for (int i = 0; i < xsize; i++) {
      xstdev += Math.pow((i - xmean), 2);
    }
    xstdev = Math.sqrt(xstdev / xsize);

    // slope is b = r sy/sx, where r = correlation
    double corr = getCorrelation();
    double slope = corr * ystdev / xstdev;
    return slope;
  }
}
