namespace Problem

open System

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

    type Config = 
        {   ProbaConvince:float; 
            ProbaMistake:float;
            ProbaScout:float;}

    let DefaultConfig = 
        {   ProbaConvince = 0.8; 
            ProbaMistake = 0.05;
            ProbaScout = 0.25; }

    let shuffle (rng:Random) xs =
        let len = xs |> Array.length
        for i in (len - 1) .. -1 .. 1 do
            let j = rng.Next(i + 1)
            let temp = xs.[j]
            xs.[j] <- xs.[i]
            xs.[i] <- temp
        xs

    let length (xs:Solution) =
        let dist (p1:Point) (p2:Point) = 
            (p1.X - p2.X) * (p1.X - p2.X) + (p1.Y - p2.Y) * (p1.Y - p2.Y)
        let len = xs |> Array.length
        xs 
        |> Seq.fold (fun (acc,prev) x -> 
            acc + dist x xs.[prev], (prev+1)%len) (0.,len-1) 
        |> fst

    let quality (xs:Solution) = - length xs

    let localSearch (rng:Random) (xs:Solution) =
        let len = xs |> Array.length
        let iters = 100
        let rec search (xs:Solution) iter =
            if iter > iters
            then xs
            else
                let i,j = rng.Next(len),rng.Next(len)
                let xs' = Array.copy xs
                xs'.[i] <- xs.[j]
                xs'.[j] <- xs.[i]
                let improvement = quality xs' > quality xs
                let mistake = rng.NextDouble () < DefaultConfig.ProbaMistake
                let next = 
                    match (improvement,mistake) with
                    | true, false -> xs'
                    | true, true -> xs
                    | false, false -> xs
                    | false, true -> xs'
                search next (iter + 1)
        search xs 0

                  