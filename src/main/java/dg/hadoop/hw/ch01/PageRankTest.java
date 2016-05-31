package dg.hadoop.hw.ch01;

import Jama.Matrix;

public class PageRankTest {

    public static void matrixPrint(Matrix matrix) {
        int rowNum = matrix.getRowDimension();
        int colNum = matrix.getColumnDimension();

        for (int j = 0; j < rowNum; j++) {
            for (int i = 0; i < colNum; i++) {
                //System.out.print(array[j][i]+"\t");
                System.out.print(matrix.get(j, i) + "\t");
                if (i == colNum - 1) {
                    System.out.print("\n");
                }
            }
        }

    }


    /**
     * <b>功能描述:</b>
     * <p>
     * <pre></pre>
     * <b>备注<b/>
     * <p>
     * <pre>add by lizc May 7, 201310:21:49 AM</pre>
     *
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub

        double weight = 0.8d;
        double n = 5d;

        // 原始google矩阵
        double[][] array = {{0, 1 / 2d, 1d / 2d, 0, 1 / 2d}, {1 / 3d, 0, 0, 0, 0}, {1 / 3d, 0, 0, 1, 1 / 2d}, {1 / 3d, 0, 0, 0, 0}, {0, 1 / 2d, 1 / 2d, 0, 0}};
        Matrix source = new Matrix(array);

        System.out.println("原始google矩阵为：");
        matrixPrint(source);


        // 原始google矩阵*weight
        source.timesEquals(weight);

        System.out.println("原始google矩阵*0.8为：");
        matrixPrint(source);


        // 单一矩阵
        Matrix singleMatrix = new Matrix(5, 5, 1d);

        // (1-weight)*(1/n)
        double weight2 = (1 - weight) * (1 / n);

        //
        singleMatrix.timesEquals(weight2);

        System.out.println("计算后的单一矩阵：");
        matrixPrint(singleMatrix);

        System.out.println("两个矩阵相加结果:");
        Matrix g = source.plus(singleMatrix);
        matrixPrint(g);


        // 结果矩阵
        Matrix q = new Matrix(5, 1, 1d);
        int i = 0;
        while (true) {

            q = g.times(q);

            i++;
            if (i > 500) {
                break;
            }


        }

        System.out.println("===============结算最终结果======================");
        matrixPrint(q);


        System.out.println("===============验算使用matix g X matrix q===============================");
        Matrix result = g.times(q);

        System.out.println("==============验算结果值为============");
        matrixPrint(result);


    }

}
