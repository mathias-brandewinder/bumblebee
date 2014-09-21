namespace Problem

module MonaLisa =

    type Point =
        struct
            val X:float
            val Y:float
            new (x:float,y:float) = { X=x; Y=y }
        end

    type Solution = Point []

    type Evaluation = 
        struct
            val Value:float
            val Solution:Solution
            new (value:float,solution:Solution) = { Value=value; Solution=solution }
        end